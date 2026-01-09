from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

# ------------------------------------------------------------
# PEAK A–U fields
# ------------------------------------------------------------
PEAK_FIELDS: List[str] = [
    "A_seq", "B_doc_date", "C_reference", "D_vendor_code", "E_tax_id_13", "F_branch_5",
    "G_invoice_no", "H_invoice_date", "I_tax_purchase_date", "J_price_type", "K_account",
    "L_description", "M_qty", "N_unit_price", "O_vat_rate", "P_wht", "Q_payment_method",
    "R_paid_amount", "S_pnd", "T_note", "U_group",
]

# Allowed enums / guidance (AI-side only; pipeline will remap later)
VENDOR_CODES = ["Shopee", "Lazada", "TikTok", "SPX", "Other"]
GROUPS = ["Marketplace Expense", "Advertising Expense", "Inventory/COGS", "Other Expense"]

# Known client tax ids (your companies)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ------------------------------------------------------------
# System prompt (STRICT JSON only)
# ------------------------------------------------------------
SYSTEM = """You are an expert Thai accounting document extractor.

Goal:
Extract accounting fields for PEAK A–U import template from Thai/English documents (invoices, tax invoices, receipts).
Return STRICT JSON only (no markdown, no explanation).

Hard rules:
- Return a JSON object with keys EXACTLY matching the provided template_fields.
- If unknown: return empty string "" (never null).
- Dates must be YYYYMMDD.
- Tax ID must be exactly 13 digits, no spaces.
- Branch must be 5 digits (pad with leading zeros; head office is 00000).
- Money fields must be plain numbers with 2 decimals if possible (e.g. 1234.50). No commas.
- Vendor_code must be one of: Shopee, Lazada, TikTok, SPX, Other.
- J_price_type: 1=แยก VAT, 2=รวม VAT, 3=ไม่มี VAT.
- O_vat_rate: '7%' or 'NO'
- U_group: choose from Marketplace Expense, Advertising Expense, Inventory/COGS, Other Expense

Quality rules:
- Prefer the document number / invoice number that represents the official tax invoice (often with platform prefixes).
- If there is a "reference code" like MMDD-XXXXXXX near the doc number, keep it (e.g. "TRS... 1203-0012589").
- Prefer vendor tax id (platform company), NOT the customer's tax id.
- If multiple totals appear, prefer Grand Total / Amount Due / Total including VAT.
- T_note must be EMPTY ("") always. Do not put any notes.
"""

# ------------------------------------------------------------
# Simple local helpers
# ------------------------------------------------------------
RE_TAX13 = re.compile(r"\b(\d{13})\b")
RE_DATE_YYYYMMDD = re.compile(r"\b(\d{8})\b")
RE_DATE_DMYYYY = re.compile(r"\b(\d{1,2})[\/\-. ](\d{1,2})[\/\-. ](\d{4})\b")
RE_DATE_YYYYMD = re.compile(r"\b(\d{4})[\/\-. ](\d{1,2})[\/\-. ](\d{1,2})\b")

# invoice + reference
RE_REF_CODE = re.compile(r"\b(\d{4}-\d{6,9})\b")
RE_DOC_WITH_REF = re.compile(r"\b([A-Z]{2,}[A-Z0-9\-/_.]{6,})\s+(\d{4}-\d{6,9})\b", re.IGNORECASE)

# Vendor word hints
RE_VENDOR_SHOPEE = re.compile(r"(shopee|ช็อปปี้|ช้อปปี้)", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"(lazada|ลาซาด้า)", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"(tiktok|ติ๊กต๊อก)", re.IGNORECASE)
RE_VENDOR_SPX = re.compile(r"(spx\s*express|standard\s*express)", re.IGNORECASE)

RE_HAS_VAT7 = re.compile(r"(vat\s*7%|ภาษีมูลค่าเพิ่ม\s*7%|vat\s*amount|total\s*vat)", re.IGNORECASE)
RE_NO_VAT = re.compile(r"(no\s*vat|ไม่มี\s*vat|vat\s*exempt|ยกเว้นภาษี)", re.IGNORECASE)

RE_PND_HINT = re.compile(r"(ภ\.ง\.ด\.?\s*53|pnd\s*53)", re.IGNORECASE)

RE_PAYMENT_DEDUCT = re.compile(r"(หักจากยอดขาย|deduct(?:ed)?\s*from\s*(?:sales|revenue))", re.IGNORECASE)
RE_PAYMENT_TRANSFER = re.compile(r"(โอน|transfer|bank\s*transfer)", re.IGNORECASE)
RE_PAYMENT_CARD = re.compile(r"(card|credit\s*card|visa|mastercard)", re.IGNORECASE)
RE_PAYMENT_CASH = re.compile(r"(cash|เงินสด)", re.IGNORECASE)


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
@dataclass
class AIExtractConfig:
    model: str = "gpt-4.1-mini"
    temperature: float = 0.0
    max_chars: int = 120_000
    timeout_s: Optional[float] = None
    enabled: bool = True


def _blank_row() -> Dict[str, Any]:
    row = {k: "" for k in PEAK_FIELDS}
    row.update({
        "A_seq": "1",
        "M_qty": "1",
        "J_price_type": "1",
        "O_vat_rate": "7%",
        "P_wht": "0",
        "R_paid_amount": "0",
        "N_unit_price": "0",
        "F_branch_5": "00000",
        "T_note": "",  # ✅ force empty note
    })
    return row


def _to_money_2(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    s = s.replace("฿", "").replace("THB", "").replace(",", "").strip()
    try:
        d = Decimal(s)
        if d < 0:
            return ""
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _to_tax13(x: Any) -> str:
    if x is None:
        return ""
    digits = "".join(c for c in str(x) if c.isdigit())
    return digits[:13] if len(digits) >= 13 else ""


def _to_branch5(x: Any) -> str:
    if x is None:
        return "00000"
    s = "".join(c for c in str(x) if c.isdigit())
    if not s:
        return "00000"
    return s.zfill(5)[:5]


def _parse_date_to_yyyymmdd(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()

    if re.fullmatch(r"\d{8}", s):
        try:
            datetime.strptime(s, "%Y%m%d")
            return s
        except Exception:
            return ""

    m = RE_DATE_YYYYMD.search(s)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        out = f"{y}{mo}{d}"
        try:
            datetime.strptime(out, "%Y%m%d")
            return out
        except Exception:
            pass

    m = RE_DATE_DMYYYY.search(s)
    if m:
        d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        out = f"{y}{mo}{d}"
        try:
            datetime.strptime(out, "%Y%m%d")
            return out
        except Exception:
            pass

    return ""


def _normalize_text_for_heuristics(text: str) -> str:
    if not text:
        return ""
    t = str(text).replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[\u200b-\u200f\ufeff]", "", t)
    lines = [re.sub(r"[ \t\f\v]+", " ", ln).strip() for ln in t.split("\n")]
    return "\n".join(lines).strip()


def _guess_vendor_code(text: str) -> str:
    t = _normalize_text_for_heuristics(text)
    if RE_VENDOR_SPX.search(t):
        return "SPX"
    if RE_VENDOR_SHOPEE.search(t):
        return "Shopee"
    if RE_VENDOR_LAZADA.search(t):
        return "Lazada"
    if RE_VENDOR_TIKTOK.search(t):
        return "TikTok"
    return "Other"


def _guess_vat_mode(text: str) -> Tuple[str, str]:
    t = _normalize_text_for_heuristics(text)
    if RE_NO_VAT.search(t):
        return ("3", "NO")
    if RE_HAS_VAT7.search(t):
        return ("1", "7%")
    return ("1", "7%")


def _guess_payment_method(text: str) -> str:
    t = _normalize_text_for_heuristics(text)
    if RE_PAYMENT_DEDUCT.search(t):
        return "หักจากยอดขาย"
    if RE_PAYMENT_TRANSFER.search(t):
        return "โอน"
    if RE_PAYMENT_CARD.search(t):
        return "CARD"
    if RE_PAYMENT_CASH.search(t):
        return "เงินสด"
    return ""


def _guess_pnd(text: str, wht_amount: str) -> str:
    t = _normalize_text_for_heuristics(text)
    if wht_amount and wht_amount not in ("0", "0.00", ""):
        if RE_PND_HINT.search(t):
            return "53"
        return "53"
    return ""


def _find_first_non_client_tax_id(text: str) -> str:
    t = _normalize_text_for_heuristics(text)
    for m in RE_TAX13.finditer(t):
        tax = m.group(1)
        if tax not in CLIENT_TAX_IDS:
            return tax
    return ""


def _find_best_doc_with_ref(text: str) -> str:
    t = _normalize_text_for_heuristics(text)
    m = RE_DOC_WITH_REF.search(t)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return ""


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _clamp_choice(val: str, allowed: List[str], fallback: str) -> str:
    v = _safe_str(val)
    return v if v in allowed else fallback


def _validate_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = _blank_row()

    for k in PEAK_FIELDS:
        if k in row:
            out[k] = _safe_str(row.get(k, ""))

    out["A_seq"] = out["A_seq"] or "1"
    out["M_qty"] = out["M_qty"] or "1"

    out["E_tax_id_13"] = _to_tax13(out.get("E_tax_id_13"))
    out["F_branch_5"] = _to_branch5(out.get("F_branch_5"))

    for dk in ["B_doc_date", "H_invoice_date", "I_tax_purchase_date"]:
        out[dk] = _parse_date_to_yyyymmdd(out.get(dk, ""))

    out["N_unit_price"] = _to_money_2(out.get("N_unit_price")) or out["N_unit_price"]
    out["R_paid_amount"] = _to_money_2(out.get("R_paid_amount")) or out["R_paid_amount"]
    out["P_wht"] = _to_money_2(out.get("P_wht")) if out.get("P_wht") not in ("", "0") else "0"

    out["D_vendor_code"] = _clamp_choice(out.get("D_vendor_code", ""), VENDOR_CODES, "Other")
    out["U_group"] = _clamp_choice(out.get("U_group", ""), GROUPS, "Other Expense")

    if out.get("J_price_type") not in ("1", "2", "3"):
        out["J_price_type"] = "1"
    if out.get("O_vat_rate") not in ("7%", "NO"):
        out["O_vat_rate"] = "7%"

    if not out["C_reference"] and out["G_invoice_no"]:
        out["C_reference"] = out["G_invoice_no"]
    if not out["G_invoice_no"] and out["C_reference"]:
        out["G_invoice_no"] = out["C_reference"]

    # ✅ Force empty note always (design requirement)
    out["T_note"] = ""

    return out


def _fill_missing_with_heuristics(row: Dict[str, Any], full_text: str) -> Dict[str, Any]:
    t = _normalize_text_for_heuristics(full_text)

    if not row.get("D_vendor_code") or row["D_vendor_code"] == "Other":
        row["D_vendor_code"] = _guess_vendor_code(t)

    if not row.get("J_price_type") or not row.get("O_vat_rate"):
        jp, vr = _guess_vat_mode(t)
        row["J_price_type"] = row.get("J_price_type") or jp
        row["O_vat_rate"] = row.get("O_vat_rate") or vr

    if not row.get("Q_payment_method"):
        row["Q_payment_method"] = _guess_payment_method(t)

    if not row.get("E_tax_id_13"):
        tax = _find_first_non_client_tax_id(t)
        if tax:
            row["E_tax_id_13"] = tax

    if not row.get("F_branch_5"):
        row["F_branch_5"] = "00000"

    if not row.get("G_invoice_no"):
        doc = _find_best_doc_with_ref(t)
        if doc:
            row["G_invoice_no"] = doc
            row["C_reference"] = row.get("C_reference") or doc

    if not row.get("B_doc_date"):
        # best-effort: take latest yyyyMMdd if any appear
        dates: List[str] = []
        for m in RE_DATE_YYYYMMDD.finditer(t):
            y = _parse_date_to_yyyymmdd(m.group(1))
            if y:
                dates.append(y)
        for m in RE_DATE_YYYYMD.finditer(t):
            y = _parse_date_to_yyyymmdd(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")
            if y:
                dates.append(y)
        for m in RE_DATE_DMYYYY.finditer(t):
            y = _parse_date_to_yyyymmdd(f"{m.group(1)}/{m.group(2)}/{m.group(3)}")
            if y:
                dates.append(y)

        if dates:
            best = max(dates)
            row["B_doc_date"] = best
            row["H_invoice_date"] = row.get("H_invoice_date") or best
            row["I_tax_purchase_date"] = row.get("I_tax_purchase_date") or best

    if (row.get("P_wht") and row["P_wht"] not in ("0", "0.00")) and not row.get("S_pnd"):
        row["S_pnd"] = _guess_pnd(t, row["P_wht"])

    if not row.get("U_group"):
        row["U_group"] = "Marketplace Expense" if row.get("D_vendor_code") in ("Shopee", "Lazada", "TikTok", "SPX") else "Other Expense"

    if not row.get("L_description"):
        if row.get("U_group") in ("Marketplace Expense", "Advertising Expense", "Inventory/COGS"):
            row["L_description"] = row["U_group"]
        else:
            row["L_description"] = "Other Expense"

    # ✅ Do NOT create notes (design requirement)
    row["T_note"] = ""

    return row


# ------------------------------------------------------------
# Main AI extraction
# ------------------------------------------------------------
def extract_with_ai(full_text: str, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    AI-assisted extraction for PEAK A–U.

    ✅ Design rules enforced:
    - If AI fails: return {"_ai_error": "..."} ONLY (no PEAK columns mutated here)
    - T_note always empty
    - Normal success returns PEAK_FIELDS only
    """
    base = _blank_row()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    cfg = AIExtractConfig(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        temperature=float(os.getenv("OPENAI_TEMPERATURE", "0") or "0"),
        max_chars=int(os.getenv("OPENAI_MAX_CHARS", "120000") or "120000"),
        enabled=(os.getenv("AI_EXTRACT_ENABLED", "1").strip() != "0"),
    )

    text = (full_text or "")[: cfg.max_chars]

    user_payload = {
        "filename": filename,
        "client_tax_id": client_tax_id or "",
        "template_fields": PEAK_FIELDS,
        "hints": {
            "vendor_code_guess": _guess_vendor_code(text),
            "vat_guess": {"J_price_type": _guess_vat_mode(text)[0], "O_vat_rate": _guess_vat_mode(text)[1]},
            "payment_guess": _guess_payment_method(text),
            "doc_with_ref_guess": _find_best_doc_with_ref(text),
            "vendor_tax_id_guess": _find_first_non_client_tax_id(text),
        },
        "document_text": text,
    }

    # If disabled or no key: return empty base (no error metadata)
    if not (cfg.enabled and api_key):
        out = _fill_missing_with_heuristics(dict(base), full_text=text)
        out = _validate_row(out)
        return {k: out.get(k, "") for k in PEAK_FIELDS}

    # ✅ Important: on ANY exception, return _ai_error metadata ONLY
    try:
        client = OpenAI(api_key=api_key)

        resp = client.chat.completions.create(
            model=cfg.model,
            temperature=cfg.temperature,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            response_format={"type": "json_object"},
        )

        content = resp.choices[0].message.content or "{}"
        ai_data = json.loads(content) if content.strip() else {}
        if not isinstance(ai_data, dict):
            ai_data = {}

        row = dict(base)
        for k in PEAK_FIELDS:
            v = ai_data.get(k, "")
            row[k] = "" if v is None else str(v).strip()

        # enforce design constraints
        row["T_note"] = ""

        row = _validate_row(row)
        row = _fill_missing_with_heuristics(row, full_text=text)
        row = _validate_row(row)

        return {k: row.get(k, "") for k in PEAK_FIELDS}

    except Exception as e:
        # ✅ Required behavior: metadata only, no PEAK columns, no T_note pollution
        msg = f"{type(e).__name__}: {str(e)}"
        msg = msg[:500]
        return {"_ai_error": msg}


__all__ = ["extract_with_ai", "PEAK_FIELDS"]
