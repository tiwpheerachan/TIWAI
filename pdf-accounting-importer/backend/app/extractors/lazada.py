# backend/app/extractors/lazada.py
"""
Lazada extractor - PEAK A-U format (Enhanced v3.0)
Supports: Marketplace service fees with 7% VAT and 3% WHT
Enhanced with:
  - Vendor code mapping (Rabbit/SHD/TopOne)
  - Smart seller code detection (TH1JHFZ0EM format)
  - Clean descriptions (Marketplace Expense)
  - Structured notes (no errors)
  - Complete fee breakdown
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
    extract_amounts,
    extract_seller_info,
    format_peak_row,
    parse_money,
)

# ========================================
# Import vendor mapping (with fallback)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        get_expense_category,
        VENDOR_LAZADA,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_LAZADA = "0105555040244"  # Lazada default Tax ID

# ============================================================
# Lazada-specific patterns (enhanced)
# ============================================================

# Document patterns (THMPTI format is primary)
RE_LAZADA_DOC_THMPTI = re.compile(
    r'\b(THMPTI\d{16})\b',
    re.IGNORECASE
)

RE_LAZADA_DOC_STRICT = re.compile(
    r'\b(THMPTI\d{16}|TH[A-Z0-9]{8,12})\b',
    re.IGNORECASE
)

RE_LAZADA_INVOICE_NO = re.compile(
    r'Invoice\s*No\.?\s*[:#：]?\s*([A-Z0-9\-/]{10,})',
    re.IGNORECASE
)

# Date patterns
RE_LAZADA_INVOICE_DATE = re.compile(
    r'Invoice\s*Date\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})',
    re.IGNORECASE
)

# Period extraction (handles both - and –)
RE_LAZADA_PERIOD = re.compile(
    r'Period\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})\s*[-–to]+\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})',
    re.IGNORECASE
)

# Seller code patterns (TH1JHFZ0EM format - Lazada specific)
RE_LAZADA_SELLER_TH_PREFIX = re.compile(
    r'\b(TH[A-Z0-9]{8,10})\b',
    re.MULTILINE
)

RE_LAZADA_SELLER_CODE = re.compile(
    r'^([A-Z0-9]{8,15})$',
    re.MULTILINE
)

# Fee line extraction (table format)
# Example: "1 Payment Fee 11,270.75"
RE_LAZADA_FEE_LINE = re.compile(
    r'^\s*(\d+)\s+(.{10,80}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$',
    re.MULTILINE | re.IGNORECASE
)

# Fee keywords (what to include)
RE_LAZADA_FEE_KEYWORDS = re.compile(
    r'(?:Payment\s*Fee|Commission|Premium\s*Package|LazCoins|Sponsored|Voucher|Marketing|Service|Discovery|Participation)',
    re.IGNORECASE
)

# WHT text extraction
RE_LAZADA_WHT_TEXT = re.compile(
    r'หักภาษี.*?ที่จ่าย.*?อัตรา.*?(\d+)\s*%.*?จำนวน\s*([0-9,]+(?:\.[0-9]{2})?)',
    re.IGNORECASE | re.DOTALL
)

# ============================================================
# Helper functions
# ============================================================

def extract_seller_code_lazada(text: str) -> str:
    """
    Extract Lazada seller code with priority
    
    Priority:
    1. TH-prefixed format (TH1JHFZ0EM) - most specific
    2. Generic seller info extraction
    3. Alphanumeric codes (8-15 chars)
    
    Returns:
        Seller code or empty string
    """
    t = normalize_text(text)
    
    # Priority 1: TH-prefixed seller code
    m = RE_LAZADA_SELLER_TH_PREFIX.search(t)
    if m:
        code = m.group(1)
        # Exclude invoice numbers
        if not code.startswith('THMPTI'):
            return code
    
    # Priority 2: Generic seller extraction from common.py
    seller_info = extract_seller_info(t)
    if seller_info['seller_code']:
        return seller_info['seller_code']
    
    # Priority 3: Generic alphanumeric code
    for m in RE_LAZADA_SELLER_CODE.finditer(t):
        code = m.group(1)
        # Filter criteria
        if code.isdigit():  # Skip pure numbers (Tax IDs)
            continue
        if code.startswith('THMPTI'):  # Skip invoice numbers
            continue
        if code.startswith('010'):  # Skip Tax IDs
            continue
        if len(code) >= 8:
            return code
    
    return ""


def extract_lazada_fee_summary(text: str, max_items: int = 8) -> Tuple[str, str]:
    """
    Extract fee breakdown from Lazada invoice
    
    Args:
        text: PDF text
        max_items: Maximum fee items to extract
    
    Returns:
        (short_summary, detailed_notes)
        
    Example:
        short = "Lazada Fees: Payment Fee, Commission, Premium Package (+3 more)"
        notes = "1. Payment Fee: ฿11,270.75\n2. Commission: ฿48,589.55\n..."
    """
    t = normalize_text(text)
    
    fee_items = []
    fee_details = []
    
    for m in RE_LAZADA_FEE_LINE.finditer(t):
        line_no = m.group(1)
        fee_desc = m.group(2).strip()
        amount = m.group(3)
        
        # Must contain fee keywords
        if not RE_LAZADA_FEE_KEYWORDS.search(fee_desc):
            continue
        
        # Exclude totals/sums
        if any(kw in fee_desc.lower() for kw in ['total', 'รวม', 'sum', 'grand', 'including', 'vat', 'tax', 'ภาษี']):
            continue
        
        # Parse and validate amount
        parsed_amount = parse_money(amount)
        if not parsed_amount or parsed_amount == '0.00':
            continue
        
        # Clean fee name
        fee_name = re.sub(r'\s{2,}', ' ', fee_desc)
        fee_name = fee_name[:60]  # Limit length
        
        fee_items.append(fee_name)
        fee_details.append(f"{line_no}. {fee_name}: ฿{parsed_amount}")
        
        if len(fee_items) >= max_items:
            break
    
    if not fee_items:
        return ('', '')
    
    # Generate short summary
    short = 'Lazada Fees: ' + ', '.join(fee_items[:3])
    if len(fee_items) > 3:
        short += f' (+{len(fee_items)-3} more)'
    
    # Generate detailed notes
    notes = '\n'.join(fee_details)
    
    return (short, notes)


def extract_wht_from_text(text: str) -> Tuple[str, str]:
    """
    Extract WHT from Lazada Thai text
    
    Example text:
    "บริษัท ลาซาด้า จำกัด ได้หักภาษีณ ที่จ่ายในอัตราร้อยละ 3% เป็นจำนวน 3,219.71 บาท"
    
    Returns:
        (rate, amount) e.g. ("3%", "3219.71")
    """
    m = RE_LAZADA_WHT_TEXT.search(text)
    if m:
        rate = f"{m.group(1)}%"
        amount = parse_money(m.group(2))
        return (rate, amount)
    
    return ('', '')


# ============================================================
# Main extraction function
# ============================================================

def extract_lazada(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract Lazada Tax Invoice/Receipt (Enhanced v3.0)
    
    Args:
        text: PDF text content
        client_tax_id: Client's Tax ID for vendor code mapping
                      (Rabbit/SHD/TopOne)
    
    Returns:
        PEAK A-U formatted dict with:
        - D: Vendor code (C00411 for Rabbit, etc.)
        - L: Short description (Marketplace Expense)
        - T: Clean notes (no AI errors)
        - All financial data complete
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    # ========================================
    # STEP 1: Vendor identification & code mapping
    # ========================================
    
    # Get Lazada Tax ID (vendor)
    vendor_tax = find_vendor_tax_id(t, 'Lazada')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    else:
        row['E_tax_id_13'] = VENDOR_LAZADA
    
    # Get vendor code (CLIENT-AWARE)
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        vendor_code = get_vendor_code(
            client_tax_id=client_tax_id,
            vendor_tax_id=row['E_tax_id_13'],
            vendor_name="Lazada"
        )
        row['D_vendor_code'] = vendor_code  # e.g., C00411 for Rabbit
    else:
        row['D_vendor_code'] = 'Lazada'  # Fallback
    
    # Branch
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    else:
        row['F_branch_5'] = '00000'
    
    # ========================================
    # STEP 2: Document number (ENHANCED)
    # ========================================
    
    invoice_no = ""
    
    # Try THMPTI format first (most specific)
    m = RE_LAZADA_DOC_THMPTI.search(t)
    if m:
        invoice_no = m.group(1)
    
    # Try strict format
    if not invoice_no:
        m = RE_LAZADA_DOC_STRICT.search(t)
        if m:
            invoice_no = m.group(1)
    
    # Try Invoice No field
    if not invoice_no:
        m = RE_LAZADA_INVOICE_NO.search(t)
        if m:
            invoice_no = m.group(1)
    
    # Use enhanced common.py (with full reference support)
    if not invoice_no:
        invoice_no = find_invoice_no(t, 'Lazada')
    
    if invoice_no:
        row['G_invoice_no'] = invoice_no
        row['C_reference'] = invoice_no
    
    # ========================================
    # STEP 3: Dates
    # ========================================
    
    date = ''
    
    # Try Invoice Date field
    m = RE_LAZADA_INVOICE_DATE.search(t)
    if m:
        date = parse_date_to_yyyymmdd(m.group(1))
    
    # Fallback to best date
    if not date:
        date = find_best_date(t)
    
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    # Extract period
    period_text = ''
    m = RE_LAZADA_PERIOD.search(t)
    if m:
        period_start = parse_date_to_yyyymmdd(m.group(1))
        period_end = parse_date_to_yyyymmdd(m.group(2))
        if period_start and period_end:
            # Format: 2025-12-01 to 2025-12-07
            period_text = f"Period: {period_start[:4]}-{period_start[4:6]}-{period_start[6:]} to {period_end[:4]}-{period_end[4:6]}-{period_end[6:]}"
    
    # ========================================
    # STEP 4: Seller information (ENHANCED)
    # ========================================
    
    seller_code = extract_seller_code_lazada(t)
    
    # ========================================
    # STEP 5: Financial amounts
    # ========================================
    
    amounts = extract_amounts(t)
    
    subtotal = amounts['subtotal']
    vat = amounts['vat']
    total = amounts['total']
    wht_rate = amounts['wht_rate'] or '3%'
    wht_amount = amounts['wht_amount']
    
    # Try to extract WHT from Thai text if not found
    if not wht_amount:
        wht_rate_thai, wht_amount_thai = extract_wht_from_text(t)
        if wht_amount_thai:
            wht_rate = wht_rate_thai
            wht_amount = wht_amount_thai
    
    # Set amounts
    row['M_qty'] = '1'
    
    if subtotal:
        row['N_unit_price'] = subtotal
    elif total:
        row['N_unit_price'] = total
    
    if total:
        row['R_paid_amount'] = total
    elif subtotal:
        row['R_paid_amount'] = subtotal
    
    # VAT
    row['J_price_type'] = '1'
    row['O_vat_rate'] = '7%'
    
    # WHT
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    # Payment method
    row['Q_payment_method'] = 'หักจากยอดขาย'
    
    # ========================================
    # STEP 6: Fee breakdown
    # ========================================
    
    short_desc, fee_notes = extract_lazada_fee_summary(t)
    
    # ========================================
    # STEP 7: Description (SHORT & CLEAN)
    # ========================================
    
    if VENDOR_MAPPING_AVAILABLE:
        # Use simple category
        row['L_description'] = "Marketplace Expense"
        row['U_group'] = "Marketplace Expense"
    else:
        # Build description
        desc_parts = []
        desc_parts.append('Lazada - Marketplace Service Fees')
        
        if short_desc:
            desc_parts.append(short_desc)
        
        if seller_code:
            desc_parts.append(f"Seller: {seller_code}")
        
        if period_text:
            desc_parts.append(period_text)
        
        if subtotal and total:
            desc_parts.append(f"Subtotal ฿{subtotal} + VAT ฿{vat} = Total ฿{total}")
        elif total:
            desc_parts.append(f"Total ฿{total}")
        
        if wht_amount:
            desc_parts.append(f"WHT {wht_rate}: ฿{wht_amount}")
        
        row['L_description'] = ' | '.join(desc_parts)
        row['U_group'] = 'Marketplace Expense'
    
    # ========================================
    # STEP 8: Notes (CLEAN & STRUCTURED)
    # ========================================
    
    note_parts = []
    
    # Seller code
    if seller_code:
        note_parts.append(f"Seller Code: {seller_code}")
    
    # Period
    if period_text:
        note_parts.append(period_text)
    
    # Fee breakdown (detailed)
    if fee_notes:
        note_parts.append('\nFee Breakdown:')
        note_parts.append(fee_notes)
    
    # Financial summary
    if subtotal and vat and total:
        note_parts.append(f"\nFinancial Summary:")
        note_parts.append(f"Subtotal: ฿{subtotal}")
        note_parts.append(f"VAT 7%: ฿{vat}")
        note_parts.append(f"Total: ฿{total}")
    
    # WHT
    if wht_rate and wht_amount:
        note_parts.append(f"\nWithholding Tax {wht_rate}: ฿{wht_amount}")
    
    # ❌ NO AI ERRORS - Keep clean!
    row['T_note'] = '\n'.join(note_parts) if note_parts else ""
    
    # ========================================
    # STEP 9: Final formatting
    # ========================================
    
    row['K_account'] = ''
    
    return format_peak_row(row)


# ============================================================
# Export
# ============================================================

__all__ = [
    'extract_lazada',
    'extract_seller_code_lazada',
    'extract_lazada_fee_summary',
    'extract_wht_from_text',
]