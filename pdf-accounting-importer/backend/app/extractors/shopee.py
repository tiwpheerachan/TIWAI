# backend/app/extractors/shopee.py
"""
Shopee extractor - PEAK A-U format (Enhanced v3.0)
Supports: Marketplace service fees with 7% VAT and 3% WHT
Enhanced with:
  - Vendor code mapping (Rabbit/SHD/TopOne)
  - Full reference number (TRSPEMKP00-00000-25 1203-0012589)
  - Smart seller code detection (TH-prefix format)
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
        VENDOR_SHOPEE,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SHOPEE = "0105558019581"  # Shopee default Tax ID

# ============================================================
# Shopee-specific patterns (enhanced)
# ============================================================

# Document patterns (TIV/TIR/TRS formats)
RE_SHOPEE_DOC_TI_FORMAT = re.compile(
    r'\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,})\b',
    re.IGNORECASE
)

RE_SHOPEE_DOC_TRS_FORMAT = re.compile(
    r'\b(TRS[A-Z0-9\-/]{10,})\b',
    re.IGNORECASE
)

RE_SHOPEE_DOC_STRICT = re.compile(
    r'\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-/]{10,})\b',
    re.IGNORECASE
)

# CRITICAL: Full reference pattern (like SPX - 2 lines)
# Example: "TRSPEMKP00-00000-25\n1203-0012589"
RE_SHOPEE_FULL_REFERENCE = re.compile(
    r'\b(TRS[A-Z0-9\-/]{10,})\s+(\d{4}-\d{7})\b',
    re.IGNORECASE
)

# Reference code alone (MMDD-NNNNNNN)
RE_SHOPEE_REFERENCE_CODE = re.compile(
    r'\b(\d{4}-\d{7})\b'
)

# Date patterns (Thai and English)
RE_SHOPEE_DOC_DATE = re.compile(
    r'(?:วันที่(?:เอกสาร|ออกเอกสาร)?|Date\s*(?:of\s*issue)?|Issue\s*date|Document\s*date)\s*[:#：]?\s*'
    r'(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})',
    re.IGNORECASE
)

RE_SHOPEE_INVOICE_DATE = re.compile(
    r'(?:วันที่ใบกำกับ(?:ภาษี)?|Invoice\s*date|Tax\s*Invoice\s*date)\s*[:#：]?\s*'
    r'(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})',
    re.IGNORECASE
)

# Seller ID patterns (enhanced)
RE_SHOPEE_SELLER_ID = re.compile(
    r'(?:Seller\s*ID|Shop\s*ID|รหัสร้านค้า)\s*[:#：]?\s*([0-9]{8,12})',
    re.IGNORECASE
)

RE_SHOPEE_USERNAME = re.compile(
    r'(?:Username|Shop\s*name|User\s*name|ชื่อผู้ใช้|ชื่อร้าน)\s*[:#：]?\s*([A-Za-z0-9_\-\.]{3,30})',
    re.IGNORECASE
)

# Fee line extraction (Shopee format)
# Example: "Commission fee 119,019.63"
RE_SHOPEE_FEE_LINE = re.compile(
    r'^\s*(.{10,80}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$',
    re.MULTILINE | re.IGNORECASE
)

# Fee keywords (Thai and English)
RE_SHOPEE_FEE_KEYWORDS = re.compile(
    r'(?:ค่าธรรมเนียม|ค่าบริการ|คอมมิชชั่น|Commission|Service\s*fee|Transaction\s*fee|Platform\s*fee'
    r'|Payment\s*fee|Shipping\s*fee|Marketing\s*fee|Voucher|Coins|Rebate|Discount|Penalty|Withdrawal|Infrastructure)',
    re.IGNORECASE
)

# WHT patterns (Thai text)
RE_SHOPEE_WHT_THAI = re.compile(
    r'(?:หัก|ภาษี).*?ที่จ่าย.*?(?:อัตรา|ร้อยละ)\s*([0-9]{1,2})\s*%.*?(?:จำนวน|เป็นเงิน)\s*([0-9,]+(?:\.[0-9]{2})?)',
    re.IGNORECASE | re.DOTALL
)

# ============================================================
# Helper functions
# ============================================================

def extract_seller_id_shopee(text: str) -> Tuple[str, str]:
    """
    Extract Shopee seller ID and username
    
    Returns:
        (seller_id, username)
        
    Example:
        seller_id = "426162640"
        username = "xiaomi.thailand"
    """
    t = normalize_text(text)
    seller_id = ""
    username = ""
    
    # Try Seller ID pattern
    m = RE_SHOPEE_SELLER_ID.search(t)
    if m:
        seller_id = m.group(1)
    
    # Try Username pattern
    m = RE_SHOPEE_USERNAME.search(t)
    if m:
        username = m.group(1)
    
    # Fallback to generic seller extraction
    if not seller_id:
        seller_info = extract_seller_info(t)
        seller_id = seller_info['seller_id']
        if not username:
            username = seller_info['username']
    
    return (seller_id, username)


def extract_shopee_full_reference(text: str) -> str:
    """
    Extract full Shopee reference number
    
    CRITICAL: Must capture BOTH document number AND reference code
    
    Examples:
        Input:  "No. TRSPEMKP00-00000-25\n1203-0012589"
        Output: "TRSPEMKP00-00000-25 1203-0012589"
        
        Input:  "TIV-ABC-12345-250103-1234567"
        Output: "TIV-ABC-12345-250103-1234567"
    
    Returns:
        Full reference or document number only if reference not found
    """
    t = normalize_text(text)
    
    # Try full reference pattern first (for TRS format with 2 lines)
    m = RE_SHOPEE_FULL_REFERENCE.search(t)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    
    # Try TIV/TIR format (single line)
    m = RE_SHOPEE_DOC_TI_FORMAT.search(t)
    if m:
        return m.group(1)
    
    # Try TRS format (single line)
    m = RE_SHOPEE_DOC_TRS_FORMAT.search(t)
    if m:
        doc_no = m.group(1)
        
        # Look for reference code nearby
        doc_pos = t.find(doc_no)
        if doc_pos != -1:
            nearby_text = t[doc_pos:doc_pos+100]
            m_ref = RE_SHOPEE_REFERENCE_CODE.search(nearby_text)
            if m_ref:
                return f"{doc_no} {m_ref.group(1)}"
        
        return doc_no
    
    # Try strict pattern
    m = RE_SHOPEE_DOC_STRICT.search(t)
    if m:
        return m.group(1)
    
    # Final fallback to enhanced common.py
    return find_invoice_no(t, 'Shopee')


def extract_shopee_fee_summary(text: str, max_items: int = 8) -> Tuple[str, str]:
    """
    Extract fee breakdown from Shopee document
    
    Args:
        text: PDF text
        max_items: Maximum fee items to extract
    
    Returns:
        (short_summary, detailed_notes)
        
    Example:
        short = "Shopee Fees: Commission, Transaction, Service (+2 more)"
        notes = "- Commission fee: ฿119,019.63\n- Transaction fee: ฿69,552.34\n..."
    """
    t = normalize_text(text)
    
    fee_items = []
    fee_details = []
    
    for m in RE_SHOPEE_FEE_LINE.finditer(t):
        line_text = m.group(1).strip()
        amount = m.group(2)
        
        # Must contain fee keywords
        if not RE_SHOPEE_FEE_KEYWORDS.search(line_text):
            continue
        
        # Exclude totals/sums/VAT lines
        if any(kw in line_text.lower() for kw in ['total', 'รวม', 'sum', 'grand', 'รวมทั้งสิ้น', 'including', 'vat 7%', 'ภาษีมูลค่าเพิ่ม']):
            continue
        
        # Parse and validate amount
        parsed_amount = parse_money(amount)
        if not parsed_amount or parsed_amount == '0.00':
            continue
        
        # Clean fee name
        fee_name = re.sub(r'\s{2,}', ' ', line_text)
        fee_name = fee_name[:60]  # Limit length
        
        fee_items.append(fee_name)
        fee_details.append(f"- {fee_name}: ฿{parsed_amount}")
        
        if len(fee_items) >= max_items:
            break
    
    if not fee_items:
        return ('', '')
    
    # Generate short summary
    short = 'Shopee Fees: ' + ', '.join(fee_items[:3])
    if len(fee_items) > 3:
        short += f' (+{len(fee_items)-3} more)'
    
    # Generate detailed notes
    notes = '\n'.join(fee_details)
    
    return (short, notes)


def extract_wht_from_shopee_text(text: str) -> Tuple[str, str]:
    """
    Extract WHT from Shopee Thai text
    
    Example text:
    "หักภาษี ณ ที่จ่าย อัตราร้อยละ 3% จำนวน 8,716.68 บาท"
    
    Returns:
        (rate, amount) e.g. ("3%", "8716.68")
    """
    m = RE_SHOPEE_WHT_THAI.search(text)
    if m:
        rate = f"{m.group(1)}%"
        amount = parse_money(m.group(2))
        return (rate, amount)
    
    return ('', '')


# ============================================================
# Main extraction function
# ============================================================

def extract_shopee(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract Shopee Tax Invoice/Statement (Enhanced v3.0)
    
    Args:
        text: PDF text content
        client_tax_id: Client's Tax ID for vendor code mapping
                      (Rabbit/SHD/TopOne)
    
    Returns:
        PEAK A-U formatted dict with:
        - D: Vendor code (C00395 for Rabbit, C00888 for SHD, C00020 for TopOne)
        - C/G: Full reference (TRSPEMKP00-00000-25 1203-0012589)
        - L: Short description (Marketplace Expense)
        - T: Clean notes (no AI errors)
        - All financial data complete
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    # ========================================
    # STEP 1: Vendor identification & code mapping
    # ========================================
    
    # Get Shopee Tax ID (vendor)
    vendor_tax = find_vendor_tax_id(t, 'Shopee')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    else:
        row['E_tax_id_13'] = VENDOR_SHOPEE
    
    # Get vendor code (CLIENT-AWARE)
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        vendor_code = get_vendor_code(
            client_tax_id=client_tax_id,
            vendor_tax_id=row['E_tax_id_13'],
            vendor_name="Shopee"
        )
        row['D_vendor_code'] = vendor_code  # e.g., C00395 for Rabbit
    else:
        row['D_vendor_code'] = 'Shopee'  # Fallback
    
    # Branch
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    else:
        row['F_branch_5'] = '00000'
    
    # ========================================
    # STEP 2: Document number (ENHANCED - with full reference!)
    # ========================================
    
    # CRITICAL: Get full reference with code
    full_reference = extract_shopee_full_reference(t)
    
    if full_reference:
        row['G_invoice_no'] = full_reference
        row['C_reference'] = full_reference
    
    # ========================================
    # STEP 3: Dates
    # ========================================
    
    date = ''
    
    # Try Document Date field
    m = RE_SHOPEE_DOC_DATE.search(t)
    if m:
        date = parse_date_to_yyyymmdd(m.group(1))
    
    # Try Invoice Date field
    if not date:
        m = RE_SHOPEE_INVOICE_DATE.search(t)
        if m:
            date = parse_date_to_yyyymmdd(m.group(1))
    
    # Fallback to best date
    if not date:
        date = find_best_date(t)
    
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    # ========================================
    # STEP 4: Seller information (ENHANCED)
    # ========================================
    
    seller_id, username = extract_seller_id_shopee(t)
    
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
        wht_rate_thai, wht_amount_thai = extract_wht_from_shopee_text(t)
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
    
    short_desc, fee_notes = extract_shopee_fee_summary(t)
    
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
        desc_parts.append('Shopee - Marketplace Service Fees')
        
        if short_desc:
            desc_parts.append(short_desc)
        
        if seller_id:
            desc_parts.append(f"Seller: {seller_id}")
        
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
    
    # Seller information
    if seller_id:
        note_parts.append(f"Seller ID: {seller_id}")
    if username:
        note_parts.append(f"Username: {username}")
    
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
    'extract_shopee',
    'extract_seller_id_shopee',
    'extract_shopee_fee_summary',
    'extract_wht_from_shopee_text',
    'extract_shopee_full_reference',
]