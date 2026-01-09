from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from ..utils.text_utils import normalize_text

# -------------------------------------------------------------------
# TikTok invoice patterns (best-effort, robust)
# Goals (FIX):
# 1) Separate amounts clearly:
#    - total_ex_vat
#    - vat_amount
#    - total_inc_vat (PRIMARY)
#    - wht_amount_3pct
# 2) Mapping MUST be:
#    row["N_unit_price"]  = total_inc_vat
#    row["R_paid_amount"] = total_inc_vat
#    row["O_vat_rate"]    = "7%"
#    row["P_wht"]         = "3%"
# 3) NEVER put wht_amount into unit_price/paid_amount
# -------------------------------------------------------------------

RE_TTSTH = re.compile(r"\bTTSTH\d{8,}\b", re.IGNORECASE)

# Prefer vendor tax id line explicitly
RE_VENDOR_TAX_LINE = re.compile(
    r"(tax\s*registration\s*number)\s*[:：\-]?\s*(\d{13})",
    re.IGNORECASE,
)

# Client/Buyer tax id line (Bill To / Tax ID) - used only to avoid picking client's id
RE_CLIENT_TAX_LINE = re.compile(
    r"\bTax\s*ID\s*[:：\-]?\s*(\d{13})\b",
    re.IGNORECASE,
)

RE_TAX_ID_13_ANY = re.compile(r"\b\d{13}\b")

# Branch (00000 / 5 digits)
RE_BRANCH_5 = re.compile(r"(branch|สาขา)\s*[:\-]?\s*(\d{1,5})", re.IGNORECASE)

# Dates
RE_DATE_ANY = re.compile(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b")  # YYYY-MM-DD or YYYY/MM/DD
RE_INVOICE_DATE_LINE = re.compile(r"(invoice\s*date)\s*[:：\-]?\s*(.+)", re.IGNORECASE)
RE_DOC_DATE_LINE = re.compile(r"(document\s*date|วันที่เอกสาร)\s*[:：\-]?\s*(.+)", re.IGNORECASE)

# TikTok commonly shows "Dec 9, 2025"
RE_DATE_MON_DD_YYYY = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})\b",
    re.IGNORECASE,
)

# Amounts
RE_MONEY = re.compile(r"(-?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|-?\d+(?:\.\d{1,2})?)")

# TikTok totals keywords (more robust)
RE_TOTAL_INCL = re.compile(
    r"(total\s*amount.*including\s*vat|total\s*amount\s*\(including\s*vat\)|amount\s*due|grand\s*total)",
    re.IGNORECASE,
)
RE_TOTAL_VAT = re.compile(r"(total\s*vat|vat\s*amount|value\s*added\s*tax)", re.IGNORECASE)
RE_SUBTOTAL_EXCL = re.compile(r"(subtotal.*excluding\s*vat|total.*excluding\s*vat|subtotal\s*\(excluding\s*vat\))", re.IGNORECASE)

# Withholding tax (WHT)
# Example: "withheld tax at the rate of 3%, amounting to ฿4,414.88"
RE_WHT_AMOUNTING = re.compile(
    r"(withheld\s*tax|withholding\s*tax).*?rate\s*of\s*(\d{1,2})\s*%.*?amounting\s*to\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)

# Sometimes written differently
RE_WHT_GENERIC = re.compile(
    r"(withheld|withholding|wht).*?(\d{1,2})\s*%.*?(?:฿|THB)?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)

# Exclude picking numbers from WHT chunks when searching totals
RE_WHT_HINT = re.compile(r"(withheld\s*tax|withholding\s*tax|wht)", re.IGNORECASE)

# Optional ads/fee hints (keep simple)
RE_ADS_HINT = re.compile(r"\b(ads|advertising|promotion|โฆษณา|ค่าโฆษณา)\b", re.IGNORECASE)


def _clean_digits(s: Any, max_len: int | None = None) -> str:
    if s is None:
        return ""
    out = "".join([c for c in str(s) if c.isdigit()])
    if max_len is not None:
        out = out[:max_len]
    return out


def _money_to_str(v: str) -> str:
    if not v:
        return ""
    s = v.strip().replace(",", "").replace("฿", "").replace("THB", "").strip()
    try:
        x = float(s)
        if x < 0:
            return ""
        return f"{x:.2f}"
    except Exception:
        return ""


def _to_yyyymmdd_from_text(s: str) -> str:
    """
    Support:
    - YYYY-MM-DD / YYYY/MM/DD
    - 'Dec 9, 2025'
    """
    if not s:
        return ""

    m = RE_DATE_ANY.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mo:02d}{d:02d}"

    m2 = RE_DATE_MON_DD_YYYY.search(s)
    if m2:
        mon_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        mon = mon_map.get(m2.group(1).lower(), 0)
        d = int(m2.group(2))
        y = int(m2.group(3))
        if 1 <= mon <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mon:02d}{d:02d}"

    return ""


def _find_amount_near_keyword_excluding(text: str, keyword_re: re.Pattern, window: int = 240) -> str:
    """
    Find amount near keyword BUT avoid chunks that are clearly about WHT.
    Picks the last number (right side) in the chunk.
    """
    if not text:
        return ""
    m = keyword_re.search(text)
    if not m:
        return ""

    start = max(0, m.start() - 60)
    end = min(len(text), m.end() + window)
    chunk = text[start:end]

    # If chunk contains WHT hint, try to cut the chunk before WHT hint to avoid picking wht amount
    w = RE_WHT_HINT.search(chunk)
    if w:
        chunk = chunk[: w.start()]

    nums = RE_MONEY.findall(chunk)
    if not nums:
        return ""
    return _money_to_str(nums[-1])


def _extract_wht_amount_3pct(text: str) -> str:
    """
    Extract WHT amount (3%) only. Returns money string like '4414.88' or ''.
    This MUST NOT be mapped into unit_price/paid_amount.
    """
    if not text:
        return ""

    m = RE_WHT_AMOUNTING.search(text)
    if m:
        rate = _clean_digits(m.group(2), 2)
        amt = _money_to_str(m.group(3))
        if rate == "3" and amt:
            return amt

    m2 = RE_WHT_GENERIC.search(text)
    if m2:
        rate = _clean_digits(m2.group(2), 2)
        amt = _money_to_str(m2.group(3))
        if rate == "3" and amt:
            return amt

    return ""


def _blank_row() -> Dict[str, Any]:
    """
    Baseline defaults for PEAK (aligned with your pipeline).
    Note: D_vendor_code is a platform label here; extract_service/vendor_mapping will convert to Cxxxxx.
    """
    return {
        "B_doc_date": "",
        "C_reference": "",
        "D_vendor_code": "TikTok",
        "E_tax_id_13": "",      # vendor tax id
        "F_branch_5": "00000",
        "G_invoice_no": "",
        "H_invoice_date": "",
        "I_tax_purchase_date": "",
        "J_price_type": "1",    # VAT separated
        "K_account": "",
        "L_description": "",
        "M_qty": "1",
        "N_unit_price": "0",
        "O_vat_rate": "7%",
        "P_wht": "0",           # we will set to "3%" if WHT exists (per your requirement)
        "Q_payment_method": "",
        "R_paid_amount": "0",
        "S_pnd": "",
        "T_note": "",           # ✅ must stay empty
        "U_group": "Marketplace Expense",
    }


def extract_tiktok(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    TikTok extractor (rule-based) - FIXED for:
    - Use Total Included VAT as primary
    - Separate total_ex_vat / vat_amount / total_inc_vat / wht_amount_3pct
    - Never let WHT amount pollute N_unit_price / R_paid_amount
    - P_wht is "3%" (rate) when found, else "0"
    - T_note always empty
    """
    t = normalize_text(text or "")
    row = _blank_row()

    if not t.strip():
        return row

    # --- Reference / invoice no ---
    tt = RE_TTSTH.search(t)
    if tt:
        ref = tt.group(0).strip()
        row["C_reference"] = ref
        row["G_invoice_no"] = ref

    # --- Vendor Tax ID (prefer Tax Registration Number) ---
    vendor_tax = ""
    m_vendor = RE_VENDOR_TAX_LINE.search(t)
    if m_vendor:
        vendor_tax = _clean_digits(m_vendor.group(2), 13)

    # Avoid choosing client's tax id as vendor tax id
    ctax = _clean_digits(client_tax_id, 13) if client_tax_id else ""
    if not vendor_tax:
        all_tax = RE_TAX_ID_13_ANY.findall(t)
        for x in all_tax:
            if ctax and x == ctax:
                continue
            vendor_tax = x
            break

    row["E_tax_id_13"] = vendor_tax

    # --- Branch 5 digits ---
    m_br = RE_BRANCH_5.search(t)
    if m_br:
        br = _clean_digits(m_br.group(2), 5)
        row["F_branch_5"] = br.zfill(5) if br else "00000"
    else:
        row["F_branch_5"] = "00000"

    # --- Dates ---
    inv_date = ""
    m_inv = RE_INVOICE_DATE_LINE.search(t)
    if m_inv:
        inv_date = _to_yyyymmdd_from_text(m_inv.group(2))
    if not inv_date:
        m_doc = RE_DOC_DATE_LINE.search(t)
        if m_doc:
            inv_date = _to_yyyymmdd_from_text(m_doc.group(2))
    if not inv_date:
        inv_date = _to_yyyymmdd_from_text(t)

    row["H_invoice_date"] = inv_date
    row["B_doc_date"] = inv_date
    row["I_tax_purchase_date"] = inv_date

    # =========================================================
    # Amount separation (STRICT)
    # =========================================================
    total_ex_vat = _find_amount_near_keyword_excluding(t, RE_SUBTOTAL_EXCL)
    vat_amount = _find_amount_near_keyword_excluding(t, RE_TOTAL_VAT)
    total_inc_vat = _find_amount_near_keyword_excluding(t, RE_TOTAL_INCL)

    # WHT amount 3% (do NOT map to totals)
    wht_amount_3pct = _extract_wht_amount_3pct(t)

    # Derive total_inc_vat if missing but subtotal+vat exist
    if (not total_inc_vat) and total_ex_vat and vat_amount:
        try:
            total_inc_vat = f"{(float(total_ex_vat) + float(vat_amount)):.2f}"
        except Exception:
            pass

    # Fallback: if still no total_inc_vat, use total_ex_vat (better than 0, still not WHT)
    if not total_inc_vat and total_ex_vat:
        total_inc_vat = total_ex_vat

    # =========================================================
    # Mapping (per your strict requirement)
    # =========================================================
    if total_inc_vat:
        row["N_unit_price"] = total_inc_vat
        row["R_paid_amount"] = total_inc_vat

    # VAT columns (always 7% in this TikTok flow per your rule)
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # WHT column: rate only "3%" (if found), else "0"
    row["P_wht"] = "3%" if wht_amount_3pct else "0"
    if wht_amount_3pct:
        row["S_pnd"] = "53"

    # --- Description / Group (keep stable) ---
    row["U_group"] = "Marketplace Expense"
    row["L_description"] = "Advertising Expense" if RE_ADS_HINT.search(t) else "Marketplace Expense"

    # ✅ T_note must be empty always
    row["T_note"] = ""

    return row
