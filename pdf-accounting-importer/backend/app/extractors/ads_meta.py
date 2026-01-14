"""
Meta Ads Receipt Extractor
Extracts data from Meta/Facebook Ads receipts
"""
from __future__ import annotations
from typing import Dict, Any
import re

def extract_meta_ads(text: str, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract PEAK row from Meta Ads receipt
    
    Pattern:
    - "Receipt for [Brand] x Shopee CPAS"
    - Account ID: xxx
    - Invoice/Payment Date: Dec 4, 2025, 11:42 AM
    - Transaction ID: 25371609625860721-25458101903878164
    - Reference Number: 8QDX88ZPM2
    - Paid: ฿30,000.00 THB
    - Meta Platforms Ireland Limited
    - VAT: Reverse charge
    """
    
    row: Dict[str, Any] = {}
    
    # ============================================================
    # Detect vendor (Meta)
    # ============================================================
    row["D_vendor_code"] = "Meta Platforms Ireland"
    row["E_tax_id_13"] = "0993000454995"  # Meta VAT ID (not 13 digits, but ok)
    row["F_branch_5"] = "00000"
    
    # ============================================================
    # Extract date
    # ============================================================
    # Pattern: "Dec 4, 2025, 11:42 AM" or "Dec 29, 2025, 6:14 AM"
    date_match = re.search(
        r"(?:Invoice/Payment Date|Payment Date)\s*\n\s*([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})",
        text,
        re.IGNORECASE
    )
    
    if date_match:
        month_str = date_match.group(1)
        day = date_match.group(2).zfill(2)
        year = date_match.group(3)
        
        months = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        month = months.get(month_str, "01")
        
        row["B_doc_date"] = f"{year}{month}{day}"
    else:
        row["B_doc_date"] = ""
    
    # ============================================================
    # Extract reference
    # ============================================================
    # Priority: Reference Number > Transaction ID
    ref_match = re.search(r"Reference Number[:\s]+([A-Z0-9]+)", text, re.IGNORECASE)
    if ref_match:
        row["C_reference"] = ref_match.group(1)
        row["G_invoice_no"] = ref_match.group(1)
    else:
        # Try Transaction ID
        tx_match = re.search(r"Transaction ID\s*\n\s*([0-9\-]+)", text, re.IGNORECASE)
        if tx_match:
            tx_id = tx_match.group(1).strip()
            # ตัดให้สั้นลง (เก็บแค่ส่วนท้าย)
            if len(tx_id) > 20:
                tx_id = tx_id[-20:]
            row["C_reference"] = tx_id
            row["G_invoice_no"] = tx_id
        else:
            row["C_reference"] = ""
            row["G_invoice_no"] = ""
    
    # ============================================================
    # Extract amount
    # ============================================================
    # Pattern: "Paid\n฿30,000.00 THB" or "฿1,847.23 THB"
    amount_match = re.search(r"Paid\s*\n\s*฿([\d,]+\.?\d*)\s*THB", text, re.IGNORECASE)
    if amount_match:
        amount_str = amount_match.group(1).replace(",", "")
        row["R_paid_amount"] = amount_str
        row["N_unit_price"] = amount_str
    else:
        row["R_paid_amount"] = ""
        row["N_unit_price"] = ""
    
    # ============================================================
    # Extract Invoice # (ถ้ามี)
    # ============================================================
    invoice_match = re.search(r"Invoice #\s*([A-Z0-9\-]+)", text, re.IGNORECASE)
    if invoice_match and not row["G_invoice_no"]:
        row["G_invoice_no"] = invoice_match.group(1)
        if not row["C_reference"]:
            row["C_reference"] = invoice_match.group(1)
    
    # ============================================================
    # Description
    # ============================================================
    row["L_description"] = "Meta Ads"
    
    # ============================================================
    # Defaults
    # ============================================================
    row["H_invoice_date"] = row["B_doc_date"]  # same as doc date
    row["I_tax_purchase_date"] = ""
    row["J_price_type"] = "1"  # รวม VAT
    row["K_account"] = ""
    row["M_qty"] = "1"
    row["O_vat_rate"] = "NO"  # Reverse charge
    row["P_wht"] = ""
    row["Q_payment_method"] = ""
    row["S_pnd"] = ""
    row["T_note"] = ""
    row["U_group"] = ""
    
    # ============================================================
    # Meta: store brand/account info
    # ============================================================
    brand_match = re.search(r"Receipt for ([^\n]+)", text, re.IGNORECASE)
    if brand_match:
        row["_brand"] = brand_match.group(1).strip()
    
    account_match = re.search(r"Account ID[:\s]+(\d+)", text, re.IGNORECASE)
    if account_match:
        row["_account_id"] = account_match.group(1).strip()
    
    return row