# backend/app/extractors/spx.py
"""
SPX Express extractor - PEAK A-U format (Enhanced v3.0)
Supports: Shipping fee receipts with 1% WHT
Enhanced with:
  - Vendor code mapping (Rabbit/SHD/TopOne)
  - Full reference number (RCSPXSPB00-00000-25 1218-0001593)
  - Clean descriptions (Shipping Expense)
  - Structured notes (no errors)
"""
from __future__ import annotations

import re
from typing import Dict, Any, Tuple

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    extract_seller_info,
    extract_amounts,
    find_payment_method,
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
        VENDOR_SPX,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SPX = "0105561164871"  # SPX default Tax ID

# ============================================================
# SPX-specific patterns (enhanced)
# ============================================================

# Receipt number patterns (with FULL reference support)
# Example: "RCSPXSPB00-00000-25\n1218-0001593"
RE_SPX_RECEIPT_NO = re.compile(
    r'(?:เลขที่|No\.?)\s*[:#：]?\s*(RCS[A-Z0-9\-/]+)',
    re.IGNORECASE
)

# CRITICAL: Full reference pattern
# Format: RCSPXSPB00-00000-25 1218-0001593
RE_SPX_FULL_REFERENCE = re.compile(
    r'\b(RCS[A-Z0-9\-/]{10,})\s+(\d{4}-\d{7})\b',
    re.IGNORECASE
)

# Reference code alone (MMDD-NNNNNNN)
RE_SPX_REFERENCE_CODE = re.compile(
    r'\b(\d{4}-\d{7})\b'
)

# Seller information
RE_SPX_SELLER_ID = re.compile(
    r'Seller\s*ID\s*[:#：]?\s*(\d{8,12})',
    re.IGNORECASE
)

RE_SPX_USERNAME = re.compile(
    r'Username\s*[:#：]?\s*([A-Za-z0-9_\-]+)',
    re.IGNORECASE
)

# Shipping fee (table format)
# Example: "1 Shipping fee 1 1,330.00 1,330.00"
RE_SPX_SHIPPING_FEE = re.compile(
    r'Shipping\s*fee\s+\d+\s+([0-9,]+\.?[0-9]*)\s+([0-9,]+\.?[0-9]*)',
    re.IGNORECASE
)

# Total amount
RE_SPX_TOTAL_AMOUNT = re.compile(
    r'(?:จำนวนเงินรวม|Total\s*amount)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE
)

# WHT patterns (1% for SPX)
RE_SPX_WHT_TH = re.compile(
    r'หักภาษีเงินได้\s*ณ\s*ที่จ่าย(?:ใน)?อัตรา(?:ร้อย)?ละ\s*(\d+)\s*%\s*เป็นจำนวนเงิน\s*([0-9,.]+)',
    re.IGNORECASE
)

RE_SPX_WHT_EN = re.compile(
    r'deducted\s+(\d+)%\s+withholding\s+tax.*?at\s+([0-9,.]+)\s+THB',
    re.IGNORECASE
)

# ============================================================
# Helper functions
# ============================================================

def extract_spx_seller_info(text: str) -> Tuple[str, str]:
    """
    Extract SPX seller ID and username
    
    Returns:
        (seller_id, username)
        
    Example:
        seller_id = "253227155"
        username = "70maiofficialstore1"
    """
    t = normalize_text(text)
    seller_id = ""
    username = ""
    
    # Try SPX-specific patterns
    m = RE_SPX_SELLER_ID.search(t)
    if m:
        seller_id = m.group(1)
    
    m = RE_SPX_USERNAME.search(t)
    if m:
        username = m.group(1)
    
    # Fallback to generic extraction
    if not seller_id or not username:
        seller_info = extract_seller_info(t)
        seller_id = seller_id or seller_info['seller_id']
        username = username or seller_info['username']
    
    return (seller_id, username)


def extract_spx_full_reference(text: str) -> str:
    """
    Extract full SPX reference number
    
    CRITICAL: Must capture BOTH document number AND reference code
    
    Examples:
        Input:  "No. RCSPXSPB00-00000-25\n1218-0001593"
        Output: "RCSPXSPB00-00000-25 1218-0001593"
        
        Input:  "Receipt: RCSPXSPB00-00000-25 1218-0001593"
        Output: "RCSPXSPB00-00000-25 1218-0001593"
    
    Returns:
        Full reference or document number only if reference not found
    """
    t = normalize_text(text)
    
    # Try full reference pattern first (MOST IMPORTANT!)
    m = RE_SPX_FULL_REFERENCE.search(t)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    
    # Try to find document and reference separately
    doc_no = ""
    ref_code = ""
    
    # Find document number
    m = RE_SPX_RECEIPT_NO.search(t)
    if m:
        doc_no = m.group(1)
    
    # Find reference code near document number
    if doc_no:
        # Look for reference code within 100 characters after doc_no
        doc_pos = t.find(doc_no)
        if doc_pos != -1:
            nearby_text = t[doc_pos:doc_pos+100]
            m = RE_SPX_REFERENCE_CODE.search(nearby_text)
            if m:
                ref_code = m.group(1)
    
    # Return combined or just document number
    if doc_no and ref_code:
        return f"{doc_no} {ref_code}"
    elif doc_no:
        return doc_no
    
    # Final fallback to enhanced common.py
    return find_invoice_no(t, 'SPX')


# ============================================================
# Main extraction function
# ============================================================

def extract_spx(text: str, client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract SPX Express receipt/invoice (Enhanced v3.0)
    
    SPX Express is a shipping service provider
    Documents are typically shipping fee receipts with 1% WHT
    
    Args:
        text: PDF text content
        client_tax_id: Client's Tax ID for vendor code mapping
                      (Rabbit/SHD/TopOne)
    
    Returns:
        PEAK A-U formatted dict with:
        - D: Vendor code (C00563 for Rabbit, C01133 for SHD, C00038 for TopOne)
        - C/G: Full reference (RCSPXSPB00-00000-25 1218-0001593)
        - L: Short description (Shipping Expense)
        - T: Clean notes (no AI errors)
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    # ========================================
    # STEP 1: Vendor identification & code mapping
    # ========================================
    
    # Get SPX Tax ID (vendor)
    vendor_tax = find_vendor_tax_id(t, 'SPX')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    else:
        row['E_tax_id_13'] = VENDOR_SPX
    
    # Get vendor code (CLIENT-AWARE)
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        vendor_code = get_vendor_code(
            client_tax_id=client_tax_id,
            vendor_tax_id=row['E_tax_id_13'],
            vendor_name="SPX"
        )
        row['D_vendor_code'] = vendor_code  # e.g., C00563 for Rabbit
    else:
        row['D_vendor_code'] = 'SPX'  # Fallback
    
    # Branch (usually Head Office)
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    else:
        row['F_branch_5'] = '00000'
    
    # ========================================
    # STEP 2: Document number (ENHANCED - with full reference!)
    # ========================================
    
    # CRITICAL: Get full reference with code
    full_reference = extract_spx_full_reference(t)
    
    if full_reference:
        row['G_invoice_no'] = full_reference
        row['C_reference'] = full_reference
    
    # ========================================
    # STEP 3: Date
    # ========================================
    
    date = find_best_date(t)
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    # ========================================
    # STEP 4: Seller/Shop Information (ENHANCED)
    # ========================================
    
    seller_id, username = extract_spx_seller_info(t)
    
    # ========================================
    # STEP 5: Amounts (Shipping Fee)
    # ========================================
    
    # Try SPX-specific pattern first
    m = RE_SPX_SHIPPING_FEE.search(t)
    if m:
        unit_price = parse_money(m.group(1))
        amount = parse_money(m.group(2))
        if unit_price:
            row['N_unit_price'] = unit_price
        if amount:
            row['R_paid_amount'] = amount
    
    # Try total amount
    if not row['R_paid_amount'] or row['R_paid_amount'] == '0':
        m = RE_SPX_TOTAL_AMOUNT.search(t)
        if m:
            total = parse_money(m.group(1))
            if total:
                row['R_paid_amount'] = total
                if not row['N_unit_price'] or row['N_unit_price'] == '0':
                    row['N_unit_price'] = total
    
    # Fallback to generic extraction
    if not row['N_unit_price'] or row['N_unit_price'] == '0':
        amounts = extract_amounts(t)
        if amounts['subtotal']:
            row['N_unit_price'] = amounts['subtotal']
        elif amounts['total']:
            row['N_unit_price'] = amounts['total']
        
        if amounts['total']:
            row['R_paid_amount'] = amounts['total']
    
    # ========================================
    # STEP 6: WHT (Withholding Tax - usually 1% for SPX)
    # ========================================
    
    wht_rate = ''
    wht_amount = ''
    
    # Try Thai pattern
    m = RE_SPX_WHT_TH.search(t)
    if m:
        wht_rate = f"{m.group(1)}%"
        wht_amount = parse_money(m.group(2))
    
    # Try English pattern
    if not wht_amount:
        m = RE_SPX_WHT_EN.search(t)
        if m:
            wht_rate = f"{m.group(1)}%"
            wht_amount = parse_money(m.group(2))
    
    # Fallback to generic extraction
    if not wht_amount:
        amounts = extract_amounts(t)
        if amounts['wht_amount']:
            wht_amount = amounts['wht_amount']
        if amounts['wht_rate']:
            wht_rate = amounts['wht_rate']
        else:
            wht_rate = '1%'  # Default for SPX
    
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    # ========================================
    # STEP 7: VAT (usually NO VAT for shipping)
    # ========================================
    
    amounts = extract_amounts(t)
    if amounts['vat']:
        row['O_vat_rate'] = '7%'
        row['J_price_type'] = '1'
    else:
        row['O_vat_rate'] = 'NO'
        row['J_price_type'] = '3'
    
    # ========================================
    # STEP 8: Description (SHORT & CLEAN)
    # ========================================
    
    if VENDOR_MAPPING_AVAILABLE:
        # Use simple category
        row['L_description'] = "Shipping Expense"
        row['U_group'] = "Shipping Expense"
    else:
        # Build description
        desc_parts = ['SPX Express - Shipping Fee']
        
        if seller_id:
            desc_parts.append(f"Seller ID: {seller_id}")
        
        if username:
            desc_parts.append(f"Shop: {username}")
        
        if wht_rate:
            desc_parts.append(f"WHT {wht_rate}: {wht_amount}")
        
        row['L_description'] = ' | '.join(desc_parts)
        row['U_group'] = 'Shipping Expense'
    
    # ========================================
    # STEP 9: Notes (CLEAN & STRUCTURED)
    # ========================================
    
    note_parts = []
    
    # Seller information
    if seller_id:
        note_parts.append(f"Seller ID: {seller_id}")
    if username:
        note_parts.append(f"Username: {username}")
    
    # Financial summary
    if row['N_unit_price'] and row['N_unit_price'] != '0':
        note_parts.append(f"\nShipping Fee: ฿{row['N_unit_price']}")
    
    # WHT
    if wht_rate and wht_amount:
        note_parts.append(f"Withholding Tax {wht_rate}: ฿{wht_amount}")
    
    # ❌ NO AI ERRORS - Keep clean!
    row['T_note'] = '\n'.join(note_parts) if note_parts else ""
    
    # ========================================
    # STEP 10: Final settings
    # ========================================
    
    # Payment Method
    row['Q_payment_method'] = 'หักจากยอดขาย'
    
    # Other Fields
    row['M_qty'] = '1'
    row['K_account'] = ''
    
    return format_peak_row(row)


# ============================================================
# Export
# ============================================================

__all__ = [
    'extract_spx',
    'extract_spx_seller_info',
    'extract_spx_full_reference',
]