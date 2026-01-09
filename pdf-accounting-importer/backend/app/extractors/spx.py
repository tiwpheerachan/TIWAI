"""
SPX Express extractor - PEAK A-U format (Enhanced v3.2 FIX AMOUNTS)
Fix goals (per your requirements):
  ✅ Avoid wrong amount root cause: NEVER put WHT amount into N_unit_price / R_paid_amount
  ✅ Separate amounts clearly:
      - total_ex_vat
      - vat_amount
      - total_inc_vat   (PRIMARY)
      - wht_amount_3pct
  ✅ Mapping must be:
      row["N_unit_price"]  = total_inc_vat
      row["R_paid_amount"] = total_inc_vat
      row["O_vat_rate"]    = "7%"
      row["P_wht"]         = "3%"
  ✅ Use Total Included VAT as the main total when available
  ✅ Clean notes (blank)
  ✅ Safe: never crash extraction, keep output stable
"""

from __future__ import annotations

import re
from typing import Dict, Any, Tuple, Optional

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_best_date,
    extract_seller_info,
    extract_amounts,
    format_peak_row,
    parse_money,
)

# ========================================
# Import vendor mapping (optional)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_SPX,
    )
    VENDOR_MAPPING_AVAILABLE = True
except Exception:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SPX = "0105561164871"  # SPX Express (Thailand) Co., Ltd. Tax ID


# ============================================================
# SPX-specific patterns
# ============================================================

# Receipt number patterns (doc no)
RE_SPX_RECEIPT_NO = re.compile(
    r"(?:เลขที่|No\.?)\s*[:#：]?\s*(RCS[A-Z0-9\-/]+)",
    re.IGNORECASE,
)

# Reference code alone (MMDD-XXXXXXX) — allow spaces around dash
RE_SPX_REF_CODE_FLEX = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")

# Full reference pattern (doc + ref) — allow whitespace/newlines between
RE_SPX_FULL_REFERENCE = re.compile(
    r"\b(RCS[A-Z0-9\-/]{10,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE,
)

# Seller information
RE_SPX_SELLER_ID = re.compile(r"Seller\s*ID\s*[:#：]?\s*(\d{8,12})", re.IGNORECASE)
RE_SPX_USERNAME  = re.compile(r"Username\s*[:#：]?\s*([A-Za-z0-9_\-]+)", re.IGNORECASE)

# Shipping fee row (table-like)  (kept as optional detail; NOT used as final totals)
RE_SPX_SHIPPING_FEE = re.compile(
    r"Shipping\s*fee\s+\d+\s+([0-9,]+(?:\.[0-9]{1,2})?)\s+([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# ---- Totals (VAT separated) ----
# Primary target: Total (including VAT)
RE_TOTAL_INC_VAT = re.compile(
    r"(?:รวม\s*ทั้ง\s*สิ้น|จำนวนเงินรวม\s*\(รวม\s*ภาษี|จำนวนเงินรวม\s*รวม\s*VAT|Total\s*(?:amount)?\s*\(?(?:including|incl\.?)\s*VAT\)?|Grand\s*Total)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Total excluding VAT
RE_TOTAL_EX_VAT = re.compile(
    r"(?:ก่อน\s*ภาษี|ยอดรวม\s*\(ไม่รวม\s*ภาษี|ยอดรวม\s*ไม่รวม\s*VAT|Subtotal\s*\(?(?:excluding|excl\.?)\s*VAT\)?|Total\s*excluding\s*VAT)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# VAT amount
RE_VAT_AMOUNT = re.compile(
    r"(?:ภาษีมูลค่าเพิ่ม|VAT)\s*(?:7\s*%|7%|@?\s*7%)?\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Fallback total amount keyword (not ideal, but ok if no inc-vat line exists)
RE_SPX_TOTAL_AMOUNT = re.compile(
    r"(?:จำนวนเงินรวม|Total\s*amount)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# ---- WHT patterns (3% per your requirement) ----
# Thai: "...อัตราร้อยละ 3 % เป็นจำนวนเงิน 1,234.56"
RE_SPX_WHT_TH = re.compile(
    r"หักภาษีเงินได้\s*ณ\s*ที่จ่าย(?:ใน)?อัตรา(?:ร้อย)?ละ\s*(\d+)\s*%\s*เป็นจำนวนเงิน\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# EN: "deducted 3% withholding tax ... at 1,234.56 THB"
RE_SPX_WHT_EN = re.compile(
    r"deducted\s+(\d+)\s*%\s+withholding\s+tax.*?\bat\s+([0-9,]+(?:\.[0-9]{1,2})?)\s+THB\b",
    re.IGNORECASE | re.DOTALL,
)

# Prevent picking WHT line as totals (heuristic)
RE_WHT_HINT = re.compile(r"(withholding\s+tax|หักภาษี|ณ\s*ที่จ่าย|wht)", re.IGNORECASE)


# ============================================================
# Helpers
# ============================================================

def _clean_ref_code(mmdd: str, seq7: str) -> str:
    return f"{mmdd}-{seq7}"


def _extract_ref_code_anywhere(t: str) -> str:
    m = RE_SPX_REF_CODE_FLEX.search(t)
    if not m:
        return ""
    return _clean_ref_code(m.group(1), m.group(2))


def _vendor_code_fallback_for_spx(client_tax_id: str) -> str:
    """
    Hard fallback mapping for SPX (per your latest rule)
    """
    cid = (client_tax_id or "").strip()
    if cid == "0105565027615":  # TopOne
        return "C00038"
    if cid == "0105563022918":  # SHD
        return "C01133"
    if cid == "0105561071873":  # Rabbit
        return "C00563"
    return "SPX"


def _get_vendor_code_safe(client_tax_id: str, vendor_tax_id: str) -> str:
    """
    Prefer vendor_mapping.get_vendor_code if available; otherwise use hard fallback.
    Must never raise.
    """
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            return get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=vendor_tax_id,
                vendor_name="SPX",
            )
        except Exception:
            return _vendor_code_fallback_for_spx(client_tax_id)
    return _vendor_code_fallback_for_spx(client_tax_id)


def extract_spx_seller_info(text: str) -> Tuple[str, str]:
    t = normalize_text(text)
    seller_id = ""
    username = ""

    m = RE_SPX_SELLER_ID.search(t)
    if m:
        seller_id = m.group(1)

    m = RE_SPX_USERNAME.search(t)
    if m:
        username = m.group(1)

    if not seller_id or not username:
        seller_info = extract_seller_info(t)
        seller_id = seller_id or (seller_info.get("seller_id") or "")
        username = username or (seller_info.get("username") or "")

    return seller_id, username


def extract_spx_full_reference(text: str, filename: str = "") -> str:
    """
    ✅ Force Full Reference = DOCNO + MMDD-XXXXXXX
    Priority:
      1) Full reference in text (doc + ref across whitespace/newlines)
      2) doc in text + ref anywhere in text
      3) full reference in filename
      4) doc in filename + ref in filename
      5) doc only (if ref missing)
      6) ""
    """
    t = normalize_text(text or "")
    fn = normalize_text(filename or "")

    # 1) Full reference in text
    m = RE_SPX_FULL_REFERENCE.search(t)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return f"{doc} {ref}"

    # 2) doc + ref anywhere in text
    doc = ""
    m_doc = RE_SPX_RECEIPT_NO.search(t)
    if m_doc:
        doc = m_doc.group(1)

    if doc:
        ref = _extract_ref_code_anywhere(t)
        if ref:
            return f"{doc} {ref}"
        return doc

    # 3) Full reference in filename
    m = RE_SPX_FULL_REFERENCE.search(fn)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return f"{doc} {ref}"

    # 4) doc + ref in filename
    m_doc = RE_SPX_RECEIPT_NO.search(fn)
    if m_doc:
        doc = m_doc.group(1)
        ref = _extract_ref_code_anywhere(fn)
        if ref:
            return f"{doc} {ref}"
        return doc

    return ""


def _extract_amounts_spx_strict(t: str) -> Tuple[str, str, str, str]:
    """
    Return (total_ex_vat, vat_amount, total_inc_vat, wht_amount_3pct)
    - Prefer explicit Inc VAT line as PRIMARY
    - Never let WHT amount override totals
    """
    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""
    wht_amount = ""

    # --- WHT first (but never used for totals) ---
    m = RE_SPX_WHT_TH.search(t)
    if m:
        rate = (m.group(1) or "").strip()
        amt = parse_money(m.group(2))
        if rate == "3" and amt:
            wht_amount = amt

    if not wht_amount:
        m = RE_SPX_WHT_EN.search(t)
        if m:
            rate = (m.group(1) or "").strip()
            amt = parse_money(m.group(2))
            if rate == "3" and amt:
                wht_amount = amt

    # --- Totals ---
    m = RE_TOTAL_INC_VAT.search(t)
    if m and not RE_WHT_HINT.search(t[max(0, m.start()-40):m.end()+40]):
        total_inc_vat = parse_money(m.group(1)) or ""

    m = RE_TOTAL_EX_VAT.search(t)
    if m and not RE_WHT_HINT.search(t[max(0, m.start()-40):m.end()+40]):
        total_ex_vat = parse_money(m.group(1)) or ""

    # VAT amount line
    # NOTE: there may be multiple VAT occurrences; take the first plausible
    m = RE_VAT_AMOUNT.search(t)
    if m and not RE_WHT_HINT.search(t[max(0, m.start()-40):m.end()+40]):
        vat_amount = parse_money(m.group(1)) or ""

    # If no explicit total_inc_vat, try fallback "Total amount" (but still avoid WHT vicinity)
    if not total_inc_vat:
        m = RE_SPX_TOTAL_AMOUNT.search(t)
        if m and not RE_WHT_HINT.search(t[max(0, m.start()-60):m.end()+60]):
            total_inc_vat = parse_money(m.group(1)) or ""

    # If still missing inc-vat, derive if possible
    if not total_inc_vat and total_ex_vat and vat_amount:
        try:
            # parse_money returns formatted string; do float best-effort
            total_inc_vat = f"{(float(total_ex_vat) + float(vat_amount)):.2f}"
        except Exception:
            pass

    # If missing ex-vat but have inc-vat + vat
    if not total_ex_vat and total_inc_vat and vat_amount:
        try:
            total_ex_vat = f"{(float(total_inc_vat) - float(vat_amount)):.2f}"
        except Exception:
            pass

    # Last resort: use common.extract_amounts but guard against WHT pollution
    if not total_inc_vat:
        am = extract_amounts(t) or {}
        # extract_amounts might be noisy; still use its 'total' only if it looks like a total
        cand_total = am.get("total") or ""
        cand_vat = am.get("vat") or ""
        cand_sub = am.get("subtotal") or ""
        if cand_total:
            total_inc_vat = cand_total
        if cand_vat and not vat_amount:
            vat_amount = cand_vat
        if cand_sub and not total_ex_vat:
            total_ex_vat = cand_sub

    return (total_ex_vat, vat_amount, total_inc_vat, wht_amount)


# ============================================================
# Main extraction
# ============================================================

def extract_spx(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Extract SPX receipt to PEAK A-U format.
    Safe: must never crash; keep output stable.
    """
    try:
        t = normalize_text(text or "")
        row = base_row_dict()

        # ========================================
        # 1) Vendor tax id (platform) + vendor code (Cxxxxx)
        # ========================================
        vendor_tax = find_vendor_tax_id(t, "SPX") or VENDOR_SPX
        row["E_tax_id_13"] = vendor_tax
        row["D_vendor_code"] = _get_vendor_code_safe(client_tax_id, vendor_tax)

        # Branch (Head Office mostly)
        row["F_branch_5"] = find_branch(t) or "00000"

        # ========================================
        # 2) Full reference (FORCED)
        # ========================================
        full_ref = extract_spx_full_reference(t, filename=filename)
        if full_ref:
            row["G_invoice_no"] = full_ref
            row["C_reference"] = full_ref

        # ========================================
        # 3) Dates (fill all 3)
        # ========================================
        date = find_best_date(t) or ""
        if date:
            row["B_doc_date"] = date
            row["H_invoice_date"] = date
            row["I_tax_purchase_date"] = date

        # ========================================
        # 4) Amounts (STRICT separation)
        # ========================================
        row["M_qty"] = "1"

        total_ex_vat, vat_amount, total_inc_vat, wht_amount_3pct = _extract_amounts_spx_strict(t)

        # ✅ Primary total = total_inc_vat only
        if total_inc_vat:
            row["N_unit_price"] = total_inc_vat
            row["R_paid_amount"] = total_inc_vat
        else:
            # very last fallback: shipping fee amount (still NOT WHT)
            m = RE_SPX_SHIPPING_FEE.search(t)
            if m:
                amount = parse_money(m.group(2))
                if amount:
                    row["N_unit_price"] = amount
                    row["R_paid_amount"] = amount

        # ========================================
        # 5) VAT/WHT mapping (per your strict rule)
        # ========================================
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"

        # ✅ P_wht is RATE only (3%) per requirement
        # If no WHT evidence found, keep 0 (safer). If you want always 3% then force it here.
        row["P_wht"] = "3%" if wht_amount_3pct else "0"
        if wht_amount_3pct:
            row["S_pnd"] = "53"

        # ========================================
        # 6) Payment method (keep your convention)
        # ========================================
        row["Q_payment_method"] = "หักจากยอดขาย"

        # ========================================
        # 7) Description + Group (keep stable + allowed)
        # ========================================
        row["L_description"] = "Marketplace Expense"
        row["U_group"] = "Marketplace Expense"

        # ========================================
        # 8) Notes must be blank
        # ========================================
        row["T_note"] = ""

        # ========================================
        # 9) Safety sync C/G
        # ========================================
        if not row.get("C_reference") and row.get("G_invoice_no"):
            row["C_reference"] = row["G_invoice_no"]
        if not row.get("G_invoice_no") and row.get("C_reference"):
            row["G_invoice_no"] = row["C_reference"]

        row["K_account"] = ""

        # IMPORTANT: we do NOT output these internal numbers into PEAK columns
        # (they are here for debugging if you later choose to expose metadata)
        # total_ex_vat, vat_amount, total_inc_vat, wht_amount_3pct

        return format_peak_row(row)

    except Exception:
        # Fail-safe: stable output, never crash
        row = base_row_dict()
        row["D_vendor_code"] = _vendor_code_fallback_for_spx(client_tax_id)
        row["F_branch_5"] = "00000"
        row["M_qty"] = "1"
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["P_wht"] = "0"
        row["R_paid_amount"] = "0"
        row["N_unit_price"] = "0"
        row["U_group"] = "Marketplace Expense"
        row["L_description"] = "Marketplace Expense"
        row["T_note"] = ""
        return format_peak_row(row)


__all__ = [
    "extract_spx",
    "extract_spx_full_reference",
    "extract_spx_seller_info",
]
