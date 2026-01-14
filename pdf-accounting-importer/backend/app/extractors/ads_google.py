"""
Google Ads Receipt Extractor
Extracts data from Google Ads payment receipts
"""
from __future__ import annotations
from typing import Dict, Any
import re

def extract_google_ads(text: str, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract PEAK row from Google Ads payment receipt
    
    Pattern:
    - "Payment Receipt" + Google logo
    - Google Asia Pacific Pte. Ltd.
    - Payment date: Dec 14, 2025
    - Billing ID: 5845-7123-1367
    - Payment number: V0971174339667745
    - Payment amount: THB 50,000.00
    """
    
    row: Dict[str, Any] = {}
    
    # ============================================================
    # Detect vendor (Google)
    # ============================================================
    row["D_vendor_code"] = "Google Asia Pacific"
    row["E_tax_id_13"] = "200817984R"  # Google Tax ID (Singapore, not 13 digits)
    row["F_branch_5"] = "00000"
    
    # ============================================================
    # Extract date
    # ============================================================
    # Pattern: "Payment date: Dec 14, 2025" or "Dec 14, 2025"
    date_match = re.search(
        r"Payment date\s*\n?\s*([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})",
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
    # Priority: Payment number > Billing ID
    payment_num_match = re.search(r"Payment number\s*\n?\s*([A-Z0-9]+)", text, re.IGNORECASE)
    if payment_num_match:
        row["C_reference"] = payment_num_match.group(1).strip()
        row["G_invoice_no"] = payment_num_match.group(1).strip()
    else:
        # Try Billing ID
        billing_match = re.search(r"Billing ID\s*\n?\s*([\d\-]+)", text, re.IGNORECASE)
        if billing_match:
            row["C_reference"] = billing_match.group(1).strip()
            row["G_invoice_no"] = billing_match.group(1).strip()
        else:
            row["C_reference"] = ""
            row["G_invoice_no"] = ""
    
    # ============================================================
    # Extract amount
    # ============================================================
    # Pattern: "Payment amount: THB 50,000.00" or "THB 50,000.00"
    amount_match = re.search(r"Payment amount\s*\n?\s*THB\s+([\d,]+\.?\d*)", text, re.IGNORECASE)
    if amount_match:
        amount_str = amount_match.group(1).replace(",", "")
        row["R_paid_amount"] = amount_str
        row["N_unit_price"] = amount_str
    else:
        # Try alternative pattern
        alt_match = re.search(r"THB\s+([\d,]+\.?\d*)", text)
        if alt_match:
            amount_str = alt_match.group(1).replace(",", "")
            row["R_paid_amount"] = amount_str
            row["N_unit_price"] = amount_str
        else:
            row["R_paid_amount"] = ""
            row["N_unit_price"] = ""
    
    # ============================================================
    # Description
    # ============================================================
    row["L_description"] = "Google Ads"
    
    # ============================================================
    # Defaults
    # ============================================================
    row["H_invoice_date"] = row["B_doc_date"]  # same as doc date
    row["I_tax_purchase_date"] = ""
    row["J_price_type"] = "1"  # รวม VAT
    row["K_account"] = ""
    row["M_qty"] = "1"
    row["O_vat_rate"] = "NO"  # Typically no VAT for Google Ads (reverse charge)
    row["P_wht"] = ""
    row["Q_payment_method"] = ""
    row["S_pnd"] = ""
    row["T_note"] = ""
    row["U_group"] = ""
    
    # ============================================================
    # Meta: store billing/payment info
    # ============================================================
    billing_match = re.search(r"Billing ID\s*\n?\s*([\d\-]+)", text, re.IGNORECASE)
    if billing_match:
        row["_billing_id"] = billing_match.group(1).strip()
    
    payment_match = re.search(r"Payment method\s*\n?\s*([^\n]+)", text, re.IGNORECASE)
    if payment_match:
        row["_payment_method"] = payment_match.group(1).strip()
    
    return row