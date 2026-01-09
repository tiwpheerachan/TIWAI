"""
Lazada extractor - PEAK A-U format (Enhanced v3.2 FIXED)
Fix goals (same class of bugs as TikTok/SPX):
  ✅ NEVER let WHT amount overwrite unit_price / paid_amount
  ✅ Separate amounts clearly:
     - total_ex_vat
     - vat_amount
     - total_inc_vat  (PRIMARY)
     - wht_amount_3pct
  ✅ Mapping (per your strict rule):
     row["N_unit_price"]  = total_inc_vat
     row["R_paid_amount"] = total_inc_vat
     row["O_vat_rate"]    = "7%"
     row["P_wht"]         = "3%"   (only if WHT exists)
  ✅ Use "Total (Including Tax)" as primary (from Lazada totals block)
  ✅ Fallbacks must avoid "wht_amount" being mistaken as totals
"""

from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_date_to_yyyymmdd,
    extract_amounts,          # fallback only
    extract_seller_info,      # fallback only
    format_peak_row,
    parse_money,
)

# ========================================
# Import vendor mapping (with fallback)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_LAZADA,
        VENDOR_NAME_MAP,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_LAZADA = "0105555040244"  # Lazada Tax ID


# ============================================================
# Lazada-specific patterns (matched to sample PDF)
# ============================================================

RE_LAZADA_SELLER_CODE_TH = re.compile(r"\b(TH[A-Z0-9]{8,12})\b")
RE_LAZADA_DOC_THMPTI = re.compile(r"\b(THMPTI\d{16})\b", re.IGNORECASE)

RE_LAZADA_INVOICE_NO_FIELD = re.compile(
    r"Invoice\s*No\.?\s*[:#：]?\s*([A-Z0-9\-/]{8,40})",
    re.IGNORECASE
)

RE_LAZADA_INVOICE_DATE = re.compile(
    r"Invoice\s*Date\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})",
    re.IGNORECASE
)

RE_LAZADA_PERIOD = re.compile(
    r"Period\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})\s*[-–]\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})",
    re.IGNORECASE
)

RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")

RE_LAZADA_FEE_LINE = re.compile(
    r"^\s*(\d+)\s+(.{3,120}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$",
    re.MULTILINE
)

# Totals block (STRICT, best source)
RE_LAZADA_SUBTOTAL_TOTAL = re.compile(
    r"^\s*Total\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_VAT_7 = re.compile(
    r"^\s*7%\s*\(VAT\)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_TOTAL_INC = re.compile(
    r"^\s*Total\s*\(Including\s*Tax\)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)

RE_LAZADA_WHT_TEXT = re.compile(
    r"หักภาษีณ?\s*ที่จ่าย.*?อัตรา(?:ร้อยละ)?\s*(\d+)\s*%.*?เป็นจำนวน\s*([0-9,]+(?:\.[0-9]{2})?)\s*บาท",
    re.IGNORECASE | re.DOTALL
)

RE_LAZADA_FEE_KEYWORDS = re.compile(
    r"(?:Payment\s*Fee|Commission|Premium\s*Package|LazCoins|Sponsored|Voucher|Marketing|Service|Discovery|Participation|Funded)",
    re.IGNORECASE
)


# ============================================================
# Helpers
# ============================================================

def _pick_client_tax_id(text: str) -> str:
    t = normalize_text(text)
    for m in RE_TAX_ID_13.finditer(t):
        tax = m.group(1)
        if tax and tax != VENDOR_LAZADA:
            return tax
    return ""


def extract_seller_code_lazada(text: str) -> str:
    t = normalize_text(text)

    candidates = []
    for m in RE_LAZADA_SELLER_CODE_TH.finditer(t):
        code = m.group(1)
        if not code.upper().startswith("THMPTI"):
            candidates.append(code)
    if candidates:
        return candidates[0]

    info = extract_seller_info(t)
    if info.get("seller_code"):
        sc = str(info["seller_code"]).strip()
        if sc and not sc.upper().startswith("THMPTI"):
            return sc

    return ""


def extract_lazada_fee_summary(text: str, max_items: int = 10) -> Tuple[str, str, List[Dict[str, str]]]:
    t = normalize_text(text)

    fee_items: List[str] = []
    fee_details: List[str] = []
    fee_list: List[Dict[str, str]] = []

    for m in RE_LAZADA_FEE_LINE.finditer(t):
        no = m.group(1)
        desc = m.group(2).strip()
        amt_raw = m.group(3)

        desc_l = desc.lower()

        # skip totals-ish
        if desc_l.startswith("total"):
            continue
        if "including tax" in desc_l or "vat" in desc_l or "tax" in desc_l or "ภาษี" in desc_l:
            continue

        if not RE_LAZADA_FEE_KEYWORDS.search(desc):
            continue

        amt = parse_money(amt_raw)
        if not amt or amt in ("0", "0.00"):
            continue

        name = re.sub(r"\s{2,}", " ", desc).strip()
        if len(name) > 90:
            name = name[:90].rstrip()

        fee_items.append(name)
        fee_details.append(f"{no}. {name}: ฿{amt}")
        fee_list.append({"no": no, "name": name, "amount": amt})

        if len(fee_items) >= max_items:
            break

    if not fee_items:
        return ("", "", [])

    short = "Lazada Fees: " + ", ".join(fee_items[:3])
    if len(fee_items) > 3:
        short += f" (+{len(fee_items) - 3} more)"

    notes = "\n".join(fee_details)
    return (short, notes, fee_list)


def extract_wht_from_text(text: str) -> Tuple[str, str]:
    """
    Returns (rate, amount) like ("3%", "3219.71")
    """
    t = normalize_text(text)
    m = RE_LAZADA_WHT_TEXT.search(t)
    if not m:
        return ("", "")
    rate = f"{m.group(1)}%"
    amt = parse_money(m.group(2)) or ""
    return (rate, amt)


def extract_totals_block(text: str) -> Tuple[str, str, str]:
    """
    Extract (total_ex_vat, vat_amount, total_inc_vat) from totals area.
    This is the PRIMARY and safest source.
    """
    t = normalize_text(text)

    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""

    m = RE_LAZADA_SUBTOTAL_TOTAL.search(t)
    if m:
        total_ex_vat = parse_money(m.group(1)) or ""

    m = RE_LAZADA_VAT_7.search(t)
    if m:
        vat_amount = parse_money(m.group(1)) or ""

    m = RE_LAZADA_TOTAL_INC.search(t)
    if m:
        total_inc_vat = parse_money(m.group(1)) or ""

    return (total_ex_vat, vat_amount, total_inc_vat)


def _safe_float(x: str) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def _derive_total_inc_vat(total_ex_vat: str, vat_amount: str) -> str:
    if not total_ex_vat or not vat_amount:
        return ""
    v = _safe_float(total_ex_vat) + _safe_float(vat_amount)
    if v <= 0:
        return ""
    return f"{v:.2f}"


# ============================================================
# Main
# ============================================================

def extract_lazada(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    FIXED mapping rule (per your latest requirement):

    Separate:
      total_ex_vat
      vat_amount
      total_inc_vat  (PRIMARY)
      wht_amount_3pct

    Mapping:
      row["N_unit_price"]  = total_inc_vat
      row["R_paid_amount"] = total_inc_vat
      row["O_vat_rate"]    = "7%"
      row["P_wht"]         = "3%"
      ❌ Never map wht_amount into totals.
    """
    t = normalize_text(text)
    row = base_row_dict()

    # --------------------------
    # STEP 1: Vendor tax & code
    # --------------------------
    vendor_tax = find_vendor_tax_id(t, "Lazada")
    row["E_tax_id_13"] = vendor_tax or VENDOR_LAZADA

    if not client_tax_id:
        client_tax_id = _pick_client_tax_id(t)

    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            row["D_vendor_code"] = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=row["E_tax_id_13"],
                vendor_name="Lazada",
            )
        except Exception:
            row["D_vendor_code"] = "Lazada"
    else:
        row["D_vendor_code"] = "Lazada"

    br = find_branch(t)
    row["F_branch_5"] = br if br else "00000"

    # --------------------------
    # STEP 2: Invoice No
    # --------------------------
    invoice_no = ""
    m = RE_LAZADA_DOC_THMPTI.search(t)
    if m:
        invoice_no = m.group(1)

    if not invoice_no:
        m = RE_LAZADA_INVOICE_NO_FIELD.search(t)
        if m:
            invoice_no = m.group(1).strip()

    if not invoice_no:
        invoice_no = find_invoice_no(t, "Lazada")

    if invoice_no:
        row["G_invoice_no"] = invoice_no
        row["C_reference"] = invoice_no

    # --------------------------
    # STEP 3: Date & Period
    # --------------------------
    doc_date = ""
    m = RE_LAZADA_INVOICE_DATE.search(t)
    if m:
        doc_date = parse_date_to_yyyymmdd(m.group(1)) or ""

    if not doc_date:
        doc_date = find_best_date(t) or ""

    if doc_date:
        row["B_doc_date"] = doc_date
        row["H_invoice_date"] = doc_date
        row["I_tax_purchase_date"] = doc_date

    period_text = ""
    m = RE_LAZADA_PERIOD.search(t)
    if m:
        p1 = parse_date_to_yyyymmdd(m.group(1)) or ""
        p2 = parse_date_to_yyyymmdd(m.group(2)) or ""
        if p1 and len(p1) == 8:
            p1_fmt = f"{p1[:4]}-{p1[4:6]}-{p1[6:]}"
        else:
            p1_fmt = m.group(1)
        if p2 and len(p2) == 8:
            p2_fmt = f"{p2[:4]}-{p2[4:6]}-{p2[6:]}"
        else:
            p2_fmt = m.group(2)
        period_text = f"Period: {p1_fmt} - {p2_fmt}"

    # --------------------------
    # STEP 4: Seller code
    # --------------------------
    seller_code = extract_seller_code_lazada(t)

    # --------------------------
    # STEP 5: Amounts (STRICT)
    # --------------------------
    total_ex_vat, vat_amount, total_inc_vat = extract_totals_block(t)

    # Derive total_inc_vat if missing but ex+vat exist (still safe)
    if not total_inc_vat:
        derived = _derive_total_inc_vat(total_ex_vat, vat_amount)
        if derived:
            total_inc_vat = derived

    # FINAL fallback: use extract_amounts BUT NEVER allow wht_amount to become total
    if not total_inc_vat:
        a = extract_amounts(t)
        # Only accept "total" if it's plausible and not equal to wht_amount
        cand_total = a.get("total", "") or ""
        cand_wht = a.get("wht_amount", "") or ""
        if cand_total and cand_total != cand_wht:
            total_inc_vat = cand_total

    # If still missing: fallback to total_ex_vat (still NOT WHT)
    if not total_inc_vat and total_ex_vat:
        total_inc_vat = total_ex_vat

    # --------------------------
    # STEP 6: WHT (STRICT 3%)
    # --------------------------
    wht_rate, wht_amount_3pct = extract_wht_from_text(t)

    # fallback from extract_amounts only if it says 3% and amount exists
    if not wht_amount_3pct:
        a = extract_amounts(t)
        r = (a.get("wht_rate", "") or "").strip()
        amt = (a.get("wht_amount", "") or "").strip()
        if r == "3%" and amt:
            wht_amount_3pct = amt
            wht_rate = "3%"

    # enforce rule: we only care about 3% here
    has_wht_3 = (wht_rate == "3%" and bool(wht_amount_3pct))

    # --------------------------
    # STEP 7: Set PEAK fields (per your strict mapping)
    # --------------------------
    row["M_qty"] = "1"

    if total_inc_vat:
        row["N_unit_price"] = total_inc_vat
        row["R_paid_amount"] = total_inc_vat
    else:
        # keep stable defaults, never use WHT amount
        row["N_unit_price"] = row.get("N_unit_price") or "0"
        row["R_paid_amount"] = row.get("R_paid_amount") or "0"

    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    row["Q_payment_method"] = "หักจากยอดขาย"

    # P_wht per your requirement = "3%" (rate), not amount
    row["P_wht"] = "3%" if has_wht_3 else "0"
    if has_wht_3:
        row["S_pnd"] = "53"

    # --------------------------
    # STEP 8: Fee breakdown (notes only)
    # --------------------------
    short_desc, fee_notes, fee_list = extract_lazada_fee_summary(t)

    # --------------------------
    # STEP 9: Description / Group
    # --------------------------
    row["L_description"] = "Marketplace Expense"
    row["U_group"] = "Marketplace Expense"

    # --------------------------
    # STEP 10: Notes (clean)
    # --------------------------
    note_parts: List[str] = []

    if seller_code:
        note_parts.append(f"Seller Code: {seller_code}")
    if client_tax_id:
        note_parts.append(f"Client Tax ID: {client_tax_id}")
    if period_text:
        note_parts.append(period_text)

    if fee_notes:
        note_parts.append("\nFee Breakdown:")
        note_parts.append(fee_notes)

    note_parts.append("\nFinancial Summary:")
    if total_ex_vat:
        note_parts.append(f"Total (ex VAT): ฿{total_ex_vat}")
    if vat_amount:
        note_parts.append(f"VAT 7%: ฿{vat_amount}")
    if total_inc_vat:
        note_parts.append(f"Total (inc VAT): ฿{total_inc_vat}")

    if has_wht_3:
        note_parts.append(f"\nWithholding Tax 3%: ฿{wht_amount_3pct}")

    row["T_note"] = "\n".join(note_parts).strip() if note_parts else ""

    # final
    row["K_account"] = ""
    return format_peak_row(row)


__all__ = [
    "extract_lazada",
    "extract_seller_code_lazada",
    "extract_lazada_fee_summary",
    "extract_wht_from_text",
    "extract_totals_block",
]
