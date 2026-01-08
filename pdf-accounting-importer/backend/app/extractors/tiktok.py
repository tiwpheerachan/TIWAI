# backend/app/extractors/tiktok.py
"""
TikTok Shop extractor - PEAK A-U format (Enhanced v3.0)
Supports: Platform service fees with 7% VAT and 3% WHT
Enhanced with:
  - Vendor code mapping (Rabbit/SHD/TopOne)
  - Clean descriptions (Marketplace Expense)
  - Structured notes (no errors)
  - Complete fee breakdown (8+ fee types)
"""
from __future__ import annotations

import re
from typing import Dict, Any, List, Tuple

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_en_date,
    extract_amounts,
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
        VENDOR_TIKTOK,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_TIKTOK = "0105566214176"  # TikTok default Tax ID

# ============================================================
# TikTok-specific patterns (enhanced)
# ============================================================

# Invoice number (TTSTH format - 14+ digits)
RE_TIKTOK_INVOICE_NO = re.compile(
    r'Invoice\s*number\s*[:#：]?\s*(TTSTH\d{14,})',
    re.IGNORECASE
)

# Date patterns (English format: Dec 9, 2025)
RE_TIKTOK_INVOICE_DATE = re.compile(
    r'Invoice\s*date\s*[:#：]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)

# Period extraction (Dec 3, 2025 - Dec 9, 2025)
RE_TIKTOK_PERIOD = re.compile(
    r'Period\s*[:#：]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\s*[-–]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)

# Client information
RE_TIKTOK_CLIENT_NAME = re.compile(
    r'Client\s*Name\s*[:#：]?\s*(.+?)(?:\s*[（(]Head\s*Office[）)])?$',
    re.IGNORECASE | re.MULTILINE
)

RE_TIKTOK_TAX_ID = re.compile(
    r'Tax\s*(?:ID|Registration\s*Number)\s*[:#：]?\s*([0-9]{13})',
    re.IGNORECASE
)

# Fee line extraction (TikTok table format)
# Example: "- BCD/FS Service Fee ฿14,726.42 ฿1,030.85 ฿15,757.27"
RE_TIKTOK_FEE_LINE = re.compile(
    r'^\s*-\s*(.+?)\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s*$',
    re.MULTILINE
)

# Financial amounts (TikTok specific)
RE_TIKTOK_SUBTOTAL = re.compile(
    r'Subtotal\s*[（(]?\s*excluding\s*VAT\s*[）)]?\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

RE_TIKTOK_VAT = re.compile(
    r'Total\s*VAT\s*7%\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

RE_TIKTOK_TOTAL = re.compile(
    r'Total\s*amount\s*[（(]?\s*including\s*VAT\s*[）)]?\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

# WHT extraction (Thai and English)
RE_TIKTOK_WHT_EN = re.compile(
    r'withheld\s+tax\s+at\s+the\s+rate\s+of\s+(\d+)%.*?amounting\s+to\s+฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE | re.DOTALL
)

RE_TIKTOK_WHT_TH = re.compile(
    r'หักภาษี\s*ณ\s*ที่จ่าย.*?อัตรา\s*(\d+)\s*%.*?จำนวน\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE | re.DOTALL
)

# ============================================================
# Helper functions
# ============================================================

def extract_tiktok_client_info(text: str) -> Tuple[str, str]:
    """
    Extract TikTok client name and Tax ID
    
    Returns:
        (client_name, client_tax_id)
    """
    t = normalize_text(text)
    client_name = ""
    client_tax_id = ""
    
    # Client name
    m = RE_TIKTOK_CLIENT_NAME.search(t)
    if m:
        client_name = m.group(1).strip()
        # Remove Thai parentheses
        client_name = client_name.replace('（', '(').replace('）', ')')
    
    # Client Tax ID (find the one that's NOT TikTok's)
    for m in RE_TIKTOK_TAX_ID.finditer(t):
        tax_id = m.group(1)
        # Skip TikTok's own Tax ID
        if tax_id != VENDOR_TIKTOK and tax_id != "0105566214176":
            client_tax_id = tax_id
            break
    
    return (client_name, client_tax_id)


def extract_tiktok_fee_summary(text: str, max_lines: int = 10) -> Tuple[str, List[Dict[str, str]]]:
    """
    Extract fee breakdown from TikTok invoice
    
    Args:
        text: PDF text
        max_lines: Maximum fee items to extract
    
    Returns:
        (short_summary, fee_list)
        
    fee_list format:
    [
        {'name': 'BCD/FS Service Fee', 'ex_vat': '14726.42', 'vat': '1030.85', 'inc_vat': '15757.27'},
        ...
    ]
    """
    t = normalize_text(text)
    
    fee_list = []
    
    for m in RE_TIKTOK_FEE_LINE.finditer(t):
        fee_name = m.group(1).strip()
        ex_vat = m.group(2).replace(',', '')
        vat = m.group(3).replace(',', '')
        inc_vat = m.group(4).replace(',', '')
        
        fee_list.append({
            'name': fee_name,
            'ex_vat': ex_vat,
            'vat': vat,
            'inc_vat': inc_vat
        })
        
        if len(fee_list) >= max_lines:
            break
    
    if not fee_list:
        return ('', [])
    
    # Generate short summary
    top_fees = [f['name'] for f in fee_list[:3]]
    short = ', '.join(top_fees)
    if len(fee_list) > 3:
        short += f' (+{len(fee_list)-3} more)'
    
    return (short, fee_list)


# ============================================================
# Main extraction function
# ============================================================

def extract_tiktok(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract TikTok Shop Tax Invoice/Receipt (Enhanced v3.0)
    
    Args:
        text: PDF text content
        client_tax_id: Client's Tax ID for vendor code mapping
                      (Rabbit/SHD/TopOne)
    
    Returns:
        PEAK A-U formatted dict with:
        - D: Vendor code (C00562 for Rabbit, C01246 for SHD, C00051 for TopOne)
        - L: Short description (Marketplace Expense)
        - T: Clean notes (no AI errors)
        - All financial data complete
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    # ========================================
    # STEP 1: Vendor identification & code mapping
    # ========================================
    
    # Get TikTok Tax ID (vendor)
    vendor_tax = find_vendor_tax_id(t, 'TikTok')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    else:
        row['E_tax_id_13'] = VENDOR_TIKTOK
    
    # Auto-detect client if not provided
    if not client_tax_id:
        _, detected_client_tax = extract_tiktok_client_info(t)
        if detected_client_tax:
            client_tax_id = detected_client_tax
    
    # Get vendor code (CLIENT-AWARE)
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        vendor_code = get_vendor_code(
            client_tax_id=client_tax_id,
            vendor_tax_id=row['E_tax_id_13'],
            vendor_name="TikTok"
        )
        row['D_vendor_code'] = vendor_code  # e.g., C00562 for Rabbit
    else:
        row['D_vendor_code'] = 'TikTok'  # Fallback
    
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
    
    # Try TikTok invoice number format (TTSTH...)
    m = RE_TIKTOK_INVOICE_NO.search(t)
    if m:
        invoice_no = m.group(1)
    
    # Use enhanced common.py (with full reference support)
    if not invoice_no:
        invoice_no = find_invoice_no(t, 'TikTok')
    
    if invoice_no:
        row['G_invoice_no'] = invoice_no
        row['C_reference'] = invoice_no
    
    # ========================================
    # STEP 3: Dates
    # ========================================
    
    date = ""
    
    # Try Invoice Date field (English format)
    m = RE_TIKTOK_INVOICE_DATE.search(t)
    if m:
        date_str = m.group(1)
        date = parse_en_date(date_str)
    
    # Fallback to best date
    if not date:
        date = find_best_date(t)
    
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    # Extract period
    period_text = ''
    m = RE_TIKTOK_PERIOD.search(t)
    if m:
        period_start = m.group(1)
        period_end = m.group(2)
        period_text = f"Period: {period_start} - {period_end}"
    
    # ========================================
    # STEP 4: Client information
    # ========================================
    
    client_name, client_tax_detected = extract_tiktok_client_info(t)
    
    # ========================================
    # STEP 5: Financial amounts (TikTok specific)
    # ========================================
    
    subtotal = ''
    vat = ''
    total = ''
    
    # Try TikTok-specific patterns first
    m = RE_TIKTOK_SUBTOTAL.search(t)
    if m:
        subtotal = parse_money(m.group(1))
    
    m = RE_TIKTOK_VAT.search(t)
    if m:
        vat = parse_money(m.group(1))
    
    m = RE_TIKTOK_TOTAL.search(t)
    if m:
        total = parse_money(m.group(1))
    
    # Fallback to generic extraction
    if not subtotal or not total:
        amounts = extract_amounts(t)
        subtotal = subtotal or amounts['subtotal']
        vat = vat or amounts['vat']
        total = total or amounts['total']
    
    # Set amounts
    row['M_qty'] = '1'
    
    if subtotal:
        row['N_unit_price'] = subtotal
        row['R_paid_amount'] = subtotal
    elif total:
        row['N_unit_price'] = total
        row['R_paid_amount'] = total
    
    # VAT
    row['J_price_type'] = '1'
    row['O_vat_rate'] = '7%'
    
    # Payment method
    row['Q_payment_method'] = 'หักจากยอดขาย'
    
    # ========================================
    # STEP 6: WHT (Withholding Tax)
    # ========================================
    
    wht_rate = ''
    wht_amount = ''
    
    # Try English pattern
    m = RE_TIKTOK_WHT_EN.search(t)
    if m:
        wht_rate = f"{m.group(1)}%"
        wht_amount = parse_money(m.group(2))
    
    # Try Thai pattern
    if not wht_amount:
        m = RE_TIKTOK_WHT_TH.search(t)
        if m:
            wht_rate = f"{m.group(1)}%"
            wht_amount = parse_money(m.group(2))
    
    # Fallback to generic extraction
    if not wht_amount:
        amounts = extract_amounts(t)
        wht_amount = amounts['wht_amount']
        wht_rate = amounts['wht_rate'] or '3%'
    
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    # ========================================
    # STEP 7: Fee breakdown
    # ========================================
    
    short_fees, fee_list = extract_tiktok_fee_summary(t)
    
    # ========================================
    # STEP 8: Description (SHORT & CLEAN)
    # ========================================
    
    if VENDOR_MAPPING_AVAILABLE:
        # Use simple category
        row['L_description'] = "Marketplace Expense"
        row['U_group'] = "Marketplace Expense"
    else:
        # Build description
        desc_parts = []
        desc_parts.append('TikTok Shop - Platform Service Fees')
        
        if short_fees:
            # Take first 2 fees only
            first_fees = ', '.join([f['name'] for f in fee_list[:2]])
            desc_parts.append(first_fees)
        
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
    # STEP 9: Notes (CLEAN & STRUCTURED)
    # ========================================
    
    note_parts = []
    
    # Client information
    if client_name:
        note_parts.append(f"Client: {client_name}")
    if client_tax_detected and client_tax_detected != row['E_tax_id_13']:
        note_parts.append(f"Client Tax ID: {client_tax_detected}")
    
    # Period
    if period_text:
        note_parts.append(f"\n{period_text}")
    
    # Fee breakdown (detailed)
    if fee_list:
        note_parts.append('\nFee Breakdown:')
        for fee in fee_list:
            note_parts.append(f"- {fee['name']}: ฿{fee['ex_vat']} + VAT ฿{fee['vat']} = ฿{fee['inc_vat']}")
    
    # Financial summary
    if subtotal and vat and total:
        note_parts.append(f"\nFinancial Summary:")
        note_parts.append(f"Subtotal (excluding VAT): ฿{subtotal}")
        note_parts.append(f"Total VAT 7%: ฿{vat}")
        note_parts.append(f"Total (including VAT): ฿{total}")
    
    # WHT
    if wht_rate and wht_amount:
        note_parts.append(f"\nWithholding Tax {wht_rate}: ฿{wht_amount}")
    
    # ❌ NO AI ERRORS - Keep clean!
    row['T_note'] = '\n'.join(note_parts) if note_parts else ""
    
    # ========================================
    # STEP 10: Final formatting
    # ========================================
    
    row['K_account'] = ''
    
    return format_peak_row(row)


# ============================================================
# Export
# ============================================================

__all__ = [
    'extract_tiktok',
    'extract_tiktok_client_info',
    'extract_tiktok_fee_summary',
]