# backend/app/services/export_service.py
from __future__ import annotations

import csv
import io
import re
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side, numbers
from openpyxl.utils import get_column_letter


# =========================
# Columns (PEAK A-U)
# =========================
COLUMNS: List[Tuple[str, str]] = [
    ("A_seq", "ลำดับที่*"),
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

# Columns that MUST be treated as TEXT in Excel (preserve leading zeros, exact strings)
TEXT_COL_KEYS = {
    "A_seq",           # sometimes treated as number, but safe to keep as text (PEAK accepts)
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "J_price_type",    # keep as text "1"/"2"/"3"
    "O_vat_rate",      # "7%" or "NO"
    "S_pnd",           # "53" etc.
}

# Numeric-like columns (we will write as numbers when safe)
NUM_COL_KEYS = {
    "M_qty",
    "N_unit_price",
    "P_wht",          # if percent like "3%" keep as text; if amount keep numeric
    "R_paid_amount",
}

# Date columns are strings "YYYYMMDD" (keep as text so PEAK import consistent)
DATE_COL_KEYS = {"B_doc_date", "H_invoice_date", "I_tax_purchase_date"}

# CSV/Excel injection prevention (when a cell starts with these, Excel may interpret as formula)
EXCEL_INJECTION_PREFIXES = ("=", "+", "-", "@")

RE_YYYYMMDD = re.compile(r"^\d{8}$")
RE_DECIMAL = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")


# =========================
# Helpers
# =========================
def _s(v: Any) -> str:
    """Safe stringify (trim)."""
    if v is None:
        return ""
    return str(v).strip()


def _escape_excel_formula(s: str) -> str:
    """
    Prevent CSV/XLSX formula injection for text cells.
    If it starts with = + - @, prefix with apostrophe.
    """
    if not s:
        return s
    if s[0] in EXCEL_INJECTION_PREFIXES:
        return "'" + s
    return s


def _as_decimal_str(s: str) -> str:
    """
    Normalize numeric string to 2 decimals if possible; else ''.
    Accepts: '1,234.5', '1234', '1234.00'
    """
    if not s:
        return ""
    x = s.replace(",", "").replace("฿", "").replace("THB", "").strip()
    try:
        d = Decimal(x)
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _to_number_or_text(key: str, raw: Any) -> Tuple[Any, str]:
    """
    Decide what value to write to XLSX and which number_format to apply.
    Returns: (value, number_format)
      - value may be str or numeric
      - number_format is an openpyxl format string (or '' meaning default)
    """
    s = _s(raw)

    # Always treat these as text (including dates in YYYYMMDD)
    if key in TEXT_COL_KEYS or key in DATE_COL_KEYS:
        return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

    # For numeric fields, try numeric, else text
    if key in NUM_COL_KEYS:
        # Special: P_wht can be "3%" (keep as text)
        if key == "P_wht":
            if "%" in s:
                return (_escape_excel_formula(s), numbers.FORMAT_TEXT)
            # else treat as amount
            norm = _as_decimal_str(s)
            if norm:
                return (float(norm), numbers.FORMAT_NUMBER_00)
            return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

        # qty can be integer-ish
        if key == "M_qty":
            # allow "1", "1.0"
            norm = _as_decimal_str(s)
            if norm:
                # if looks integer, write int
                try:
                    f = float(norm)
                    if abs(f - int(f)) < 1e-9:
                        return (int(f), "0")
                    return (f, numbers.FORMAT_NUMBER_00)
                except Exception:
                    pass
            return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

        # money
        norm = _as_decimal_str(s)
        if norm:
            return (float(norm), numbers.FORMAT_NUMBER_00)
        return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

    # Default: plain text
    return (_escape_excel_formula(s), numbers.FORMAT_TEXT)


def _auto_fit_columns(ws, max_width: int = 60, min_width: int = 10) -> None:
    """
    Approximate auto-fit width (works ok for Thai/English mixed).
    """
    for col_idx, (key, label) in enumerate(COLUMNS, start=1):
        col_letter = get_column_letter(col_idx)
        max_len = len(str(label))

        # Check body values
        for row_idx in range(2, ws.max_row + 1):
            v = ws.cell(row=row_idx, column=col_idx).value
            if v is None:
                continue
            s = str(v)
            # slightly reduce long multi-line notes
            if "\n" in s:
                s = s.split("\n", 1)[0]
            max_len = max(max_len, len(s))

        # heuristic: Thai tends to be wider; add padding
        width = int(min(max(max_len + 2, min_width), max_width))
        ws.column_dimensions[col_letter].width = width


# =========================
# CSV Export
# =========================
def export_rows_to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    """
    Export PEAK import CSV (UTF-8-SIG) with:
    - Excel injection protection for text cells
    - Preserve leading zeros for critical fields by keeping them as strings
    """
    out = io.StringIO()
    wri = csv.writer(out, quoting=csv.QUOTE_MINIMAL)

    wri.writerow([label for _, label in COLUMNS])

    for r in rows:
        row_out: List[str] = []
        for k, _label in COLUMNS:
            s = _s(r.get(k, ""))
            # Protect against formula injection in CSV
            s = _escape_excel_formula(s)
            row_out.append(s)
        wri.writerow(row_out)

    return out.getvalue().encode("utf-8-sig")


# =========================
# XLSX Export
# =========================
def export_rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    """
    Export PEAK import XLSX with:
    - Freeze header
    - Auto-filter
    - Nice header style
    - Text formatting for critical columns (Tax ID, Branch, Invoice No, Reference, Vendor Code)
    - Numeric formatting for amounts when possible
    - Excel injection protection for text cells
    - Auto-fit column widths
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "PEAK_IMPORT"

    # Header row
    headers = [label for _, label in COLUMNS]
    ws.append(headers)

    # Header styling
    header_fill = PatternFill("solid", fgColor="E8F1FF")  # light blue glass-ish
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
    for r in rows:
        values: List[Any] = []
        formats: List[str] = []
        for k, _label in COLUMNS:
            v, fmt = _to_number_or_text(k, r.get(k, ""))
            values.append(v)
            formats.append(fmt)

        ws.append(values)

        # Apply per-cell number formats for the row we just appended
        current_row = ws.max_row
        for col_idx, fmt in enumerate(formats, start=1):
            cell = ws.cell(row=current_row, column=col_idx)
            if fmt:
                cell.number_format = fmt
            # Style body a bit
            cell.alignment = Alignment(vertical="top", wrap_text=(col_idx in {12, 20}))  # L_description, T_note
            cell.border = border

    # Freeze panes below header
    ws.freeze_panes = "A2"

    # Auto filter on header row
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLUMNS))}1"

    # Make sure all TEXT_COL_KEYS are forced to text format (even if empty)
    col_index = {k: i + 1 for i, (k, _) in enumerate(COLUMNS)}
    for key in TEXT_COL_KEYS | DATE_COL_KEYS:
        ci = col_index.get(key)
        if not ci:
            continue
        for row_i in range(2, 2 + len(rows)):
            cell = ws.cell(row=row_i, column=ci)
            cell.number_format = numbers.FORMAT_TEXT

    # Auto-fit widths
    _auto_fit_columns(ws)

    # Save to bytes
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


__all__ = [
    "COLUMNS",
    "export_rows_to_csv_bytes",
    "export_rows_to_xlsx_bytes",
]
