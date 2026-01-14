# backend/app/services/export_service.py
"""
Export Service - Final Enhanced Version

✅ Combines all features:
1. ✅ Platform-aware processing (8 platforms)
2. ✅ Smart date parsing (5+ formats → YYYYMMDD)
3. ✅ Smart amount parsing (฿, THB, commas → Decimal)
4. ✅ Platform detection (4-level)
5. ✅ Auto-correction (Meta, Google)
6. ✅ Platform-specific validation
7. ✅ Enhanced summary with platform breakdown
8. ✅ Metadata preservation
9. ✅ Comprehensive error handling
10. ✅ Excel injection prevention
11. ✅ Auto-fit columns
12. ✅ Validation with detailed errors
"""
from __future__ import annotations

import csv
import io
import re
import logging
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Tuple, Optional, Set

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side, numbers
from openpyxl.utils import get_column_letter

# Setup logger
logger = logging.getLogger(__name__)

# =========================
# Platform Constants (aligned with ai_service.py)
# =========================

# ✅ Platform-specific vendor codes
PLATFORM_VENDORS = {
    "META": "Meta Platforms Ireland",
    "GOOGLE": "Google Asia Pacific",
    "SHOPEE": "Shopee",
    "LAZADA": "Lazada",
    "TIKTOK": "TikTok",
    "SPX": "Shopee Express",
    "THAI_TAX": "",  # Variable (from document)
    "UNKNOWN": "Other",
}

# ✅ Platform-specific VAT rules
PLATFORM_VAT_RULES = {
    "META": {"J_price_type": "3", "O_vat_rate": "NO"},
    "GOOGLE": {"J_price_type": "3", "O_vat_rate": "NO"},
    "SHOPEE": {"J_price_type": "1", "O_vat_rate": "7%"},
    "LAZADA": {"J_price_type": "1", "O_vat_rate": "7%"},
    "TIKTOK": {"J_price_type": "1", "O_vat_rate": "7%"},
    "SPX": {"J_price_type": "1", "O_vat_rate": "7%"},
    "THAI_TAX": {"J_price_type": "1", "O_vat_rate": "7%"},
    "UNKNOWN": {"J_price_type": "1", "O_vat_rate": "7%"},
}

# ✅ Platform-specific groups
PLATFORM_GROUPS = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Delivery/Logistics Expense",
    "THAI_TAX": "General Expense",
    "UNKNOWN": "Other Expense",
}

# =========================
# Columns
# =========================
COLUMNS: List[Tuple[str, str]] = [
    ("A_seq", "ลำดับที่*"),
    ("A_company_name", "ชื่อบริษัท"),
    ("B_doc_date", "วันที่เอกสาร"),
    ("C_reference", "อ้างอิงถึง"),
    ("D_vendor_code", "ผู้รับเงิน/คู่ค้า"),
    ("E_tax_id_13", "เลขทะเบียน 13 หลัก"),
    ("F_branch_5", "เลขสาขา 5 หลัก"),
    ("G_invoice_no", "เลขที่ใบกำกับฯ (ถ้ามี)"),
    ("H_invoice_date", "วันที่ใบกำกับฯ (ถ้ามี)"),
    ("I_tax_purchase_date", "วันที่บันทึกภาษีซื้อ (ถ้ามี)"),
    ("J_price_type", "ประเภทราคา"),
    ("K_account", "บัญชี"),
    ("L_description", "คำอธิบาย"),
    ("M_qty", "จำนวน"),
    ("N_unit_price", "ราคาต่อหน่วย"),
    ("O_vat_rate", "อัตราภาษี"),
    ("P_wht", "หัก ณ ที่จ่าย (ถ้ามี)"),
    ("Q_payment_method", "ชำระโดย"),
    ("R_paid_amount", "จำนวนเงินที่ชำระ"),
    ("S_pnd", "ภ.ง.ด. (ถ้ามี)"),
    ("T_note", "หมายเหตุ"),
    ("U_group", "กลุ่มจัดประเภท"),
]

# Columns that MUST be TEXT in Excel
TEXT_COL_KEYS: Set[str] = {
    "A_seq",
    "A_company_name",
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "J_price_type",
    "O_vat_rate",
    "S_pnd",
    "Q_payment_method",
}

# Numeric columns
NUM_COL_KEYS: Set[str] = {
    "M_qty",
    "N_unit_price",
    "R_paid_amount",
}

# Date columns (YYYYMMDD format)
DATE_COL_KEYS: Set[str] = {"B_doc_date", "H_invoice_date", "I_tax_purchase_date"}

# CSV/Excel injection prevention
EXCEL_INJECTION_PREFIXES = ("=", "+", "-", "@")

# Regex patterns
RE_YYYYMMDD = re.compile(r"^\d{8}$")
RE_YYYY_MM_DD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
RE_DD_MM_YYYY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
RE_YYYY_SLASH_MM_DD = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")
RE_DECIMAL = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
RE_ALL_WS = re.compile(r"\s+")

# Limits for safety
MAX_ROWS = 50000
MAX_CELL_LENGTH = 32767

# =========================
# Validation
# =========================
class ExportValidationError(Exception):
    """Raised when export data validation fails"""
    pass


def validate_rows(rows: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Validate rows before export
    
    Returns:
        (is_valid, error_messages)
    """
    errors = []
    
    if not rows:
        errors.append("No rows to export")
        return (False, errors)
    
    if len(rows) > MAX_ROWS:
        errors.append(f"Too many rows: {len(rows)} (max: {MAX_ROWS})")
        return (False, errors)
    
    # Check each row
    for idx, row in enumerate(rows[:100], start=1):
        if not isinstance(row, dict):
            errors.append(f"Row {idx}: Not a dictionary")
            continue
        
        # Check required keys
        if not any(row.get(k) for k, _ in COLUMNS):
            errors.append(f"Row {idx}: All fields empty")
    
    if errors:
        return (False, errors)
    
    return (True, [])


# =========================
# Helpers
# =========================
def _s(v: Any) -> str:
    """Safe string conversion"""
    if v is None:
        return ""
    try:
        s = str(v).strip()
        if len(s) > MAX_CELL_LENGTH:
            logger.warning(f"Cell value truncated (was {len(s)} chars)")
            s = s[:MAX_CELL_LENGTH]
        return s
    except Exception as e:
        logger.error(f"String conversion error: {e}")
        return ""


def _escape_excel_formula(s: str) -> str:
    """Prevent Excel formula injection"""
    if not s:
        return s
    try:
        if s[0] in EXCEL_INJECTION_PREFIXES:
            return "'" + s
        return s
    except Exception:
        return s


def _compact_no_ws(v: Any) -> str:
    """Remove all whitespace from value"""
    s = _s(v)
    if not s:
        return ""
    try:
        return RE_ALL_WS.sub("", s)
    except Exception as e:
        logger.error(f"Compact whitespace error: {e}")
        return s


# =========================
# Smart Parsing Functions
# =========================

def _parse_date_to_yyyymmdd(date_str: Any) -> str:
    """
    ✅ Parse multiple date formats to YYYYMMDD
    
    Supports:
    - YYYYMMDD (already correct)
    - YYYY-MM-DD
    - DD/MM/YYYY (Thai format)
    - YYYY/MM/DD
    - Timestamps
    
    Returns:
        YYYYMMDD string or empty string
    """
    if not date_str:
        return ""
    
    try:
        s = _s(date_str)
        
        # Already YYYYMMDD
        if RE_YYYYMMDD.match(s):
            return s
        
        # YYYY-MM-DD
        m = RE_YYYY_MM_DD.match(s)
        if m:
            return f"{m.group(1)}{m.group(2)}{m.group(3)}"
        
        # DD/MM/YYYY (Thai format)
        m = RE_DD_MM_YYYY.match(s)
        if m:
            return f"{m.group(3)}{m.group(2)}{m.group(1)}"
        
        # YYYY/MM/DD
        m = RE_YYYY_SLASH_MM_DD.match(s)
        if m:
            return f"{m.group(1)}{m.group(2)}{m.group(3)}"
        
        # Try datetime parsing as fallback
        from datetime import datetime
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y%m%d")
            except ValueError:
                continue
        
        # Cannot parse
        logger.warning(f"Cannot parse date: {s}")
        return ""
    
    except Exception as e:
        logger.error(f"Date parsing error: {date_str} - {e}")
        return ""


def _parse_amount(amount_str: Any) -> str:
    """
    ✅ Parse amount to decimal string (2 decimal places)
    
    Handles:
    - ฿30,000.00 → 30000.00
    - THB 50000 → 50000.00
    - 1,234.56 → 1234.56
    - 1000 → 1000.00
    
    Returns:
        Decimal string or empty string
    """
    if not amount_str:
        return ""
    
    try:
        s = _s(amount_str)
        # Remove currency symbols and formatting
        s = s.replace("฿", "").replace("THB", "").replace("$", "")
        s = s.replace(",", "").replace(" ", "").strip()
        
        if not s:
            return ""
        
        # Parse to Decimal
        d = Decimal(s)
        if d < 0:
            logger.warning(f"Negative amount: {amount_str}")
            return ""
        
        return f"{d:.2f}"
    
    except (InvalidOperation, ValueError) as e:
        logger.warning(f"Amount parsing error: {amount_str} - {e}")
        return ""


# =========================
# Platform Detection
# =========================

def _detect_platform(row: Dict[str, Any]) -> str:
    """
    ✅ Detect platform from row data (4-level hierarchy)
    
    Priority:
    1. U_group (if set by extractor)
    2. _route_name metadata
    3. D_vendor_code pattern matching
    4. _extraction_method
    
    Returns:
        Platform key (META, GOOGLE, SHOPEE, etc.)
    """
    try:
        # Level 1: Check U_group
        group = _s(row.get("U_group", ""))
        if "advertising" in group.lower():
            vendor = _s(row.get("D_vendor_code", "")).lower()
            if "meta" in vendor or "facebook" in vendor:
                return "META"
            if "google" in vendor:
                return "GOOGLE"
        
        # Level 2: Check _route_name (from router)
        route = _s(row.get("_route_name", "")).upper()
        if route in PLATFORM_VENDORS:
            return route
        
        # Level 3: Check D_vendor_code
        vendor = _s(row.get("D_vendor_code", "")).lower()
        if "meta" in vendor or "facebook" in vendor:
            return "META"
        if "google" in vendor:
            return "GOOGLE"
        if "shopee express" in vendor or vendor == "spx":
            return "SPX"
        if "shopee" in vendor:
            return "SHOPEE"
        if "lazada" in vendor:
            return "LAZADA"
        if "tiktok" in vendor:
            return "TIKTOK"
        
        # Level 4: Check _extraction_method
        method = _s(row.get("_extraction_method", "")).lower()
        if "meta" in method:
            return "META"
        if "google" in method:
            return "GOOGLE"
        if "spx" in method:
            return "SPX"
        
        # Check for Thai Tax Invoice indicators
        if row.get("E_tax_id_13") and len(_s(row.get("E_tax_id_13", ""))) == 13:
            return "THAI_TAX"
        
        return "UNKNOWN"
    
    except Exception as e:
        logger.error(f"Platform detection error: {e}")
        return "UNKNOWN"


# =========================
# Auto-Correction Functions
# =========================

def _auto_correct_meta_ads(row: Dict[str, Any]) -> Dict[str, Any]:
    """✅ Auto-correct Meta Ads fields"""
    try:
        row["D_vendor_code"] = PLATFORM_VENDORS["META"]
        row["O_vat_rate"] = "NO"
        row["J_price_type"] = "3"
        row["U_group"] = PLATFORM_GROUPS["META"]
        
        # Preserve metadata in T_note
        notes = []
        if row.get("T_note"):
            notes.append(_s(row["T_note"]))
        notes.append("Platform: META (Auto-corrected)")
        if row.get("_extraction_method"):
            notes.append(f"Method: {row['_extraction_method']}")
        
        row["T_note"] = "\n".join(notes)[:1500]
        
        logger.debug("✅ Auto-corrected Meta Ads row")
        return row
    
    except Exception as e:
        logger.error(f"Meta auto-correction error: {e}")
        return row


def _auto_correct_google_ads(row: Dict[str, Any]) -> Dict[str, Any]:
    """✅ Auto-correct Google Ads fields"""
    try:
        row["D_vendor_code"] = PLATFORM_VENDORS["GOOGLE"]
        row["O_vat_rate"] = "NO"
        row["J_price_type"] = "3"
        row["U_group"] = PLATFORM_GROUPS["GOOGLE"]
        
        # Preserve metadata in T_note
        notes = []
        if row.get("T_note"):
            notes.append(_s(row["T_note"]))
        notes.append("Platform: GOOGLE (Auto-corrected)")
        if row.get("_extraction_method"):
            notes.append(f"Method: {row['_extraction_method']}")
        
        row["T_note"] = "\n".join(notes)[:1500]
        
        logger.debug("✅ Auto-corrected Google Ads row")
        return row
    
    except Exception as e:
        logger.error(f"Google auto-correction error: {e}")
        return row


def _auto_correct_platform(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """✅ Apply platform-specific auto-corrections"""
    try:
        if platform == "META":
            return _auto_correct_meta_ads(row)
        elif platform == "GOOGLE":
            return _auto_correct_google_ads(row)
        else:
            # For other platforms, just ensure vendor is correct
            if platform in PLATFORM_VENDORS and PLATFORM_VENDORS[platform]:
                if not row.get("D_vendor_code"):
                    row["D_vendor_code"] = PLATFORM_VENDORS[platform]
            
            # Ensure group is set
            if platform in PLATFORM_GROUPS:
                if not row.get("U_group"):
                    row["U_group"] = PLATFORM_GROUPS[platform]
        
        return row
    
    except Exception as e:
        logger.error(f"Platform auto-correction error: {e}")
        return row


# =========================
# Platform Validation
# =========================

def _validate_meta_ads(row: Dict[str, Any]) -> List[str]:
    """✅ Validate Meta Ads row"""
    errors = []
    
    if row.get("D_vendor_code") != PLATFORM_VENDORS["META"]:
        errors.append(f"META: Incorrect vendor (expected '{PLATFORM_VENDORS['META']}')")
    
    if row.get("O_vat_rate") != "NO":
        errors.append("META: VAT rate must be 'NO' (reverse charge)")
    
    if not row.get("C_reference"):
        errors.append("META: Missing receipt ID (C_reference)")
    
    if not row.get("R_paid_amount"):
        errors.append("META: Missing amount (R_paid_amount)")
    
    return errors


def _validate_google_ads(row: Dict[str, Any]) -> List[str]:
    """✅ Validate Google Ads row"""
    errors = []
    
    if row.get("D_vendor_code") != PLATFORM_VENDORS["GOOGLE"]:
        errors.append(f"GOOGLE: Incorrect vendor (expected '{PLATFORM_VENDORS['GOOGLE']}')")
    
    if row.get("O_vat_rate") != "NO":
        errors.append("GOOGLE: VAT rate must be 'NO' (international)")
    
    if not row.get("C_reference"):
        errors.append("GOOGLE: Missing payment number (C_reference)")
    
    if not row.get("R_paid_amount"):
        errors.append("GOOGLE: Missing amount (R_paid_amount)")
    
    return errors


def _validate_thai_tax(row: Dict[str, Any]) -> List[str]:
    """✅ Validate Thai Tax Invoice row"""
    errors = []
    
    tax_id = _s(row.get("E_tax_id_13", ""))
    if not tax_id or len(tax_id) != 13:
        errors.append("THAI_TAX: Invalid or missing 13-digit tax ID")
    
    branch = _s(row.get("F_branch_5", ""))
    if not branch or len(branch) != 5:
        errors.append("THAI_TAX: Invalid or missing 5-digit branch code")
    
    if not row.get("G_invoice_no"):
        errors.append("THAI_TAX: Missing invoice number (recommended)")
    
    return errors


def _validate_platform_specific(row: Dict[str, Any], platform: str) -> List[str]:
    """✅ Run platform-specific validation"""
    try:
        if platform == "META":
            return _validate_meta_ads(row)
        elif platform == "GOOGLE":
            return _validate_google_ads(row)
        elif platform == "THAI_TAX":
            return _validate_thai_tax(row)
        else:
            return []
    
    except Exception as e:
        logger.error(f"Platform validation error: {e}")
        return []


# =========================
# Preprocessing Pipeline
# =========================

def _preprocess_rows_for_export(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    ✅ Complete preprocessing pipeline with platform awareness
    
    Pipeline:
    1. Detect platform
    2. Auto-correct platform fields
    3. Renumber sequences
    4. Parse dates (5+ formats → YYYYMMDD)
    5. Parse amounts (฿, THB → Decimal)
    6. Compact references
    7. Validate platform rules
    8. Force P_wht = ""
    
    Args:
        rows: Raw rows from extraction
    
    Returns:
        Preprocessed rows ready for export
    """
    out: List[Dict[str, Any]] = []
    seq = 1
    
    logger.info(f"Starting preprocessing for {len(rows)} rows...")

    for idx, r in enumerate(rows or [], start=1):
        try:
            rr = dict(r)

            # ========== Step 1: Detect Platform ==========
            platform = _detect_platform(rr)
            logger.debug(f"Row {idx}: Platform = {platform}")

            # ========== Step 2: Auto-Correct Platform Fields ==========
            rr = _auto_correct_platform(rr, platform)

            # ========== Step 3: Renumber Sequence ==========
            rr["A_seq"] = str(seq)
            seq += 1

            # ========== Step 4: Parse Dates (Smart) ==========
            for date_key in DATE_COL_KEYS:
                if date_key in rr:
                    rr[date_key] = _parse_date_to_yyyymmdd(rr[date_key])

            # ========== Step 5: Parse Amounts (Smart) ==========
            for amt_key in ["R_paid_amount", "N_unit_price"]:
                if amt_key in rr:
                    rr[amt_key] = _parse_amount(rr[amt_key])

            # ========== Step 6: Compact References ==========
            rr["C_reference"] = _compact_no_ws(rr.get("C_reference", ""))
            rr["G_invoice_no"] = _compact_no_ws(rr.get("G_invoice_no", ""))

            # ========== Step 7: Validate Platform Rules ==========
            validation_errors = _validate_platform_specific(rr, platform)
            if validation_errors:
                logger.warning(f"Row {idx} validation warnings: {'; '.join(validation_errors)}")
                # Add to T_note
                notes = []
                if rr.get("T_note"):
                    notes.append(_s(rr["T_note"]))
                notes.append(f"Warnings: {'; '.join(validation_errors[:3])}")
                rr["T_note"] = "\n".join(notes)[:1500]

            # ========== Step 8: Force P_wht = "" ==========
            rr["P_wht"] = ""

            # ========== Step 9: Ensure Critical Fields ==========
            rr["A_company_name"] = _s(rr.get("A_company_name", ""))
            for key in ["D_vendor_code", "E_tax_id_13", "F_branch_5"]:
                if rr.get(key) is None:
                    rr[key] = ""

            out.append(rr)

        except Exception as e:
            logger.error(f"Error preprocessing row {idx}: {e}", exc_info=True)
            continue

    logger.info(f"✅ Preprocessing complete: {len(out)}/{len(rows)} rows ready")
    return out


def _to_number_or_text(key: str, raw: Any) -> Tuple[Any, str]:
    """Convert value to appropriate Excel type"""
    try:
        s = _s(raw)

        # Always treat these as text
        if key in TEXT_COL_KEYS or key in DATE_COL_KEYS:
            return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

        # For numeric fields
        if key in NUM_COL_KEYS:
            if key == "M_qty":
                norm = _parse_amount(s)
                if norm:
                    try:
                        f = float(norm)
                        if abs(f - int(f)) < 1e-9:
                            return (int(f), "0")
                        return (f, numbers.FORMAT_NUMBER_00)
                    except Exception:
                        pass
                return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

            # Money fields
            norm = _parse_amount(s)
            if norm:
                try:
                    return (float(norm), numbers.FORMAT_NUMBER_00)
                except Exception:
                    pass
            return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

        # Default: plain text
        return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

    except Exception as e:
        logger.error(f"Type conversion error for {key}: {e}")
        return ("", numbers.FORMAT_TEXT)


def _auto_fit_columns(ws, max_width: int = 60, min_width: int = 10) -> None:
    """Auto-fit column widths based on content"""
    try:
        for col_idx, (_key, label) in enumerate(COLUMNS, start=1):
            col_letter = get_column_letter(col_idx)
            max_len = len(str(label))

            max_check = min(ws.max_row + 1, 102)
            for row_idx in range(2, max_check):
                try:
                    v = ws.cell(row=row_idx, column=col_idx).value
                    if v is None:
                        continue
                    s = str(v)
                    if "\n" in s:
                        s = s.split("\n", 1)[0]
                    max_len = max(max_len, len(s))
                except Exception:
                    continue

            width = int(min(max(max_len + 2, min_width), max_width))
            ws.column_dimensions[col_letter].width = width

    except Exception as e:
        logger.error(f"Auto-fit columns error: {e}")


# =========================
# CSV Export
# =========================
def export_rows_to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    """Export rows to CSV bytes with UTF-8 BOM"""
    try:
        logger.info(f"Starting CSV export for {len(rows)} rows")

        # Validate
        is_valid, errors = validate_rows(rows)
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"Validation failed: {error_msg}")
            raise ExportValidationError(error_msg)

        # Preprocess
        rows2 = _preprocess_rows_for_export(rows)

        if not rows2:
            raise ExportValidationError("No valid rows after preprocessing")

        # Export
        out = io.StringIO()
        wri = csv.writer(out, quoting=csv.QUOTE_MINIMAL)

        # Header
        wri.writerow([label for _, label in COLUMNS])

        # Data rows
        for r in rows2:
            row_out: List[str] = []
            for k, _label in COLUMNS:
                s = _s(r.get(k, ""))
                s = _escape_excel_formula(s)
                row_out.append(s)
            wri.writerow(row_out)

        result = out.getvalue().encode("utf-8-sig")
        logger.info(f"✅ CSV export complete: {len(result)} bytes")
        return result

    except ExportValidationError:
        raise
    except Exception as e:
        logger.error(f"CSV export error: {e}", exc_info=True)
        raise Exception(f"CSV export failed: {str(e)}")


# =========================
# XLSX Export
# =========================
def export_rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    """Export rows to XLSX bytes"""
    try:
        logger.info(f"Starting XLSX export for {len(rows)} rows")

        # Validate
        is_valid, errors = validate_rows(rows)
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"Validation failed: {error_msg}")
            raise ExportValidationError(error_msg)

        # Preprocess
        rows2 = _preprocess_rows_for_export(rows)

        if not rows2:
            raise ExportValidationError("No valid rows after preprocessing")

        # Create workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "PEAK_IMPORT"

        # Header row
        headers = [label for _, label in COLUMNS]
        ws.append(headers)

        # Header styling
        header_fill = PatternFill("solid", fgColor="E8F1FF")
        header_font = Font(bold=True)
        header_align = Alignment(vertical="center", horizontal="center", wrap_text=True)

        thin = Side(style="thin", color="D0D7E2")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)

        for col_idx in range(1, len(COLUMNS) + 1):
            c = ws.cell(row=1, column=col_idx)
            c.fill = header_fill
            c.font = header_font
            c.alignment = header_align
            c.border = border

        # Data rows
        for row_num, r in enumerate(rows2, start=2):
            try:
                values: List[Any] = []
                formats: List[str] = []

                for k, _label in COLUMNS:
                    v, fmt = _to_number_or_text(k, r.get(k, ""))
                    values.append(v)
                    formats.append(fmt)

                ws.append(values)

                current_row = ws.max_row
                for col_idx, fmt in enumerate(formats, start=1):
                    cell = ws.cell(row=current_row, column=col_idx)
                    if fmt:
                        cell.number_format = fmt
                    cell.alignment = Alignment(vertical="top", wrap_text=(col_idx in {13, 21}))
                    cell.border = border

            except Exception as e:
                logger.error(f"Error writing row {row_num}: {e}")
                continue

        # Freeze panes and filter
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(COLUMNS))}1"

        # Force TEXT formatting for critical columns
        col_index = {k: i + 1 for i, (k, _) in enumerate(COLUMNS)}
        for key in (TEXT_COL_KEYS | DATE_COL_KEYS):
            ci = col_index.get(key)
            if not ci:
                continue
            for row_i in range(2, 2 + len(rows2)):
                try:
                    cell = ws.cell(row=row_i, column=ci)
                    cell.number_format = numbers.FORMAT_TEXT
                except Exception:
                    continue

        # Auto-fit columns
        _auto_fit_columns(ws)

        # Save to bytes
        bio = io.BytesIO()
        wb.save(bio)
        result = bio.getvalue()

        logger.info(f"✅ XLSX export complete: {len(result)} bytes")
        return result

    except ExportValidationError:
        raise
    except Exception as e:
        logger.error(f"XLSX export error: {e}", exc_info=True)
        raise Exception(f"XLSX export failed: {str(e)}")


# =========================
# Utility Functions
# =========================
def get_export_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    ✅ Get comprehensive export summary with platform breakdown
    
    Returns:
        Summary dict with detailed stats
    """
    try:
        summary = {
            "total_rows": len(rows),
            "valid_rows": 0,
            "platforms": {},
            "extraction_methods": {},
            "clients": {},
            "date_range": {"earliest": None, "latest": None},
            "total_amount": 0.0,
            "warnings": [],
        }

        for row in rows:
            # Count valid rows
            if row.get("D_vendor_code"):
                summary["valid_rows"] += 1

            # Platform breakdown (by U_group)
            group = row.get("U_group", "Unknown")
            summary["platforms"][group] = summary["platforms"].get(group, 0) + 1

            # Extraction method breakdown
            method = row.get("_extraction_method", "unknown")
            summary["extraction_methods"][method] = summary["extraction_methods"].get(method, 0) + 1

            # Client breakdown
            company = row.get("A_company_name", "Unknown")
            summary["clients"][company] = summary["clients"].get(company, 0) + 1

            # Date range
            doc_date = row.get("B_doc_date")
            if doc_date:
                if not summary["date_range"]["earliest"] or doc_date < summary["date_range"]["earliest"]:
                    summary["date_range"]["earliest"] = doc_date
                if not summary["date_range"]["latest"] or doc_date > summary["date_range"]["latest"]:
                    summary["date_range"]["latest"] = doc_date

            # Total amount
            amount_str = row.get("R_paid_amount", "")
            if amount_str:
                try:
                    amount = float(_parse_amount(amount_str))
                    summary["total_amount"] += amount
                except Exception:
                    pass

            # Collect warnings
            if row.get("_validation_warnings"):
                summary["warnings"].extend(row["_validation_warnings"][:2])

        # Limit warnings
        summary["warnings"] = summary["warnings"][:10]

        logger.info(f"Export summary: {summary['valid_rows']}/{summary['total_rows']} valid rows")
        return summary

    except Exception as e:
        logger.error(f"Error getting export summary: {e}")
        return {"error": str(e)}


__all__ = [
    "COLUMNS",
    "PLATFORM_VENDORS",
    "PLATFORM_VAT_RULES",
    "PLATFORM_GROUPS",
    "export_rows_to_csv_bytes",
    "export_rows_to_xlsx_bytes",
    "ExportValidationError",
    "validate_rows",
    "get_export_summary",
]