# backend/app/services/ai_service.py
from __future__ import annotations

import json
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

import requests

# ---------------------------------------------------------------------
# Constants / Schema
# ---------------------------------------------------------------------

PEAK_FIELDS = [
    "A_seq", "B_doc_date", "C_reference", "D_vendor_code", "E_tax_id_13", "F_branch_5",
    "G_invoice_no", "H_invoice_date", "I_tax_purchase_date", "J_price_type", "K_account",
    "L_description", "M_qty", "N_unit_price", "O_vat_rate", "P_wht", "Q_payment_method",
    "R_paid_amount", "S_pnd", "T_note", "U_group",
]

VENDOR_CODES = {"Shopee", "Lazada", "TikTok", "SPX", "Other"}
VAT_RATES = {"7%", "NO"}
PRICE_TYPES = {"1", "2", "3"}
PND_ALLOWED = {"", "1", "2", "3", "53"}

# ✅ Fix: define groups (used by U_group)
GROUPS = {
    "Marketplace Expense",
    "Advertising Expense",
    "Inventory/COGS",
    "Other Expense",
}

# Known client tax ids (your companies)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ---------------------------------------------------------------------
# Heuristics regex
# ---------------------------------------------------------------------

RE_VENDOR_SHOPEE = re.compile(r"(shopee|ช็อปปี้|ช้อปปี้)", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"(lazada|ลาซาด้า)", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"(tiktok|ติ๊กต๊อก)", re.IGNORECASE)
RE_VENDOR_SPX = re.compile(r"(spx\s*express|standard\s*express)", re.IGNORECASE)

RE_TAX13 = re.compile(r"\b(\d{13})\b")

RE_HAS_VAT7 = re.compile(
    r"(vat\s*7%|ภาษีมูลค่าเพิ่ม\s*7%|total\s*vat|vat\s*amount|ภาษีมูลค่าเพิ่ม)",
    re.IGNORECASE,
)
RE_NO_VAT = re.compile(r"(no\s*vat|ไม่มี\s*vat|vat\s*exempt|ยกเว้นภาษี)", re.IGNORECASE)

RE_PAYMENT_DEDUCT = re.compile(r"(หักจากยอดขาย|deduct(?:ed)?\s*from\s*(?:sales|revenue))", re.IGNORECASE)
RE_PAYMENT_TRANSFER = re.compile(r"(โอน|transfer|bank\s*transfer)", re.IGNORECASE)
RE_PAYMENT_CARD = re.compile(r"(card|credit\s*card|visa|mastercard)", re.IGNORECASE)
RE_PAYMENT_CASH = re.compile(r"(cash|เงินสด)", re.IGNORECASE)

RE_WHT_RATE = re.compile(r"(?:อัตรา|rate|ร้อยละ)\s*([0-9]{1,2})\s*%", re.IGNORECASE)
RE_WHT_ANY = re.compile(r"(withholding|wht|หักภาษี|ณ\s*ที่จ่าย)", re.IGNORECASE)
RE_PND_HINT = re.compile(r"(ภ\.ง\.ด\.?\s*53|pnd\s*53)", re.IGNORECASE)

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    t = str(text).replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[\u200b-\u200f\ufeff]", "", t)
    lines = [re.sub(r"[ \t\f\v]+", " ", ln).strip() for ln in t.split("\n")]
    return "\n".join(lines).strip()


def _to_tax13(v: Any) -> str:
    if v is None:
        return ""
    digits = "".join(c for c in str(v) if c.isdigit())
    return digits[:13] if len(digits) >= 13 else ""


def _to_branch5(v: Any) -> str:
    if v is None:
        return "00000"
    digits = "".join(c for c in str(v) if c.isdigit())
    if not digits:
        return "00000"
    return digits.zfill(5)[:5]


def _to_money_2(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    s = s.replace("฿", "").replace("THB", "").replace(",", "").strip()
    try:
        d = Decimal(s)
        if d < 0:
            return ""
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _clamp_choice(v: Any, allowed: set[str], fallback: str) -> str:
    s = "" if v is None else str(v).strip()
    return s if s in allowed else fallback


def _first_json_object(s: str) -> Optional[str]:
    """Best-effort: extract first JSON object from a model output."""
    if not s:
        return None
    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        return s
    m = re.search(r"\{[\s\S]*\}", s)
    return m.group(0) if m else None


def _guess_vendor_code(text: str, hint: str = "") -> str:
    t = _normalize_text(text)
    h = (hint or "").strip().lower()

    if "spx" in h:
        return "SPX"
    if "shopee" in h:
        return "Shopee"
    if "lazada" in h:
        return "Lazada"
    if "tiktok" in h:
        return "TikTok"

    if RE_VENDOR_SPX.search(t):
        return "SPX"
    if RE_VENDOR_SHOPEE.search(t):
        return "Shopee"
    if RE_VENDOR_LAZADA.search(t):
        return "Lazada"
    if RE_VENDOR_TIKTOK.search(t):
        return "TikTok"

    return "Other"


def _guess_vat(text: str) -> Tuple[str, str]:
    t = _normalize_text(text)
    if RE_NO_VAT.search(t):
        return "3", "NO"
    if RE_HAS_VAT7.search(t):
        return "1", "7%"
    return "1", "7%"


def _guess_payment_method(text: str) -> str:
    t = _normalize_text(text)
    if RE_PAYMENT_DEDUCT.search(t):
        return "หักจากยอดขาย"
    if RE_PAYMENT_TRANSFER.search(t):
        return "โอน"
    if RE_PAYMENT_CARD.search(t):
        return "CARD"
    if RE_PAYMENT_CASH.search(t):
        return "เงินสด"
    return ""


def _guess_vendor_tax_id(text: str) -> str:
    """Pick first 13-digit Tax ID that is NOT one of the client tax ids."""
    t = _normalize_text(text)
    for m in RE_TAX13.finditer(t):
        tax = m.group(1)
        if tax not in CLIENT_TAX_IDS:
            return tax
    return ""


def _guess_pnd(text: str, wht: str) -> str:
    t = _normalize_text(text)
    w = _to_money_2(wht)
    if w and w not in ("0.00", ""):
        if RE_PND_HINT.search(t):
            return "53"
        return "53"
    return ""


def _wht_percent_to_amount(unit_price: str, wht_field: str) -> str:
    """If model returns '3%' convert to amount from N_unit_price."""
    if not unit_price:
        return ""
    m = re.search(r"([0-9]{1,2})(?:\s*)%", str(wht_field))
    if not m:
        return ""
    try:
        rate = Decimal(m.group(1)) / Decimal("100")
        base = Decimal(str(unit_price).replace(",", ""))
        if base <= 0:
            return ""
        return f"{(base * rate):.2f}"
    except Exception:
        return ""


def _truncate_text_smart(text: str, max_len: int) -> str:
    """Keep head+tail for long OCR texts."""
    t = (text or "").strip()
    if len(t) <= max_len:
        return t
    head = int(max_len * 0.65)
    tail = max_len - head - 40
    return t[:head] + "\n\n...<TRUNCATED>...\n\n" + t[-tail:]


# ---------------------------------------------------------------------
# OpenAI caller (requests)
# ---------------------------------------------------------------------

def _openai_chat_json(system: str, user: str, model: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY")

    base_url = os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
    url = base_url.rstrip("/") + "/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "response_format": {"type": "json_object"},
    }

    timeout = float(os.getenv("OPENAI_TIMEOUT", "90") or "90")
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    content = ""
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        content = ""

    js = _first_json_object(content) or "{}"
    try:
        obj = json.loads(js)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def ai_fill_peak_row(
    text: str,
    platform_hint: str,
    partial_row: Dict[str, Any],
    source_filename: str,
) -> Dict[str, Any]:
    """
    LLM step: fill PEAK columns from OCR/text.

    Enabled when ENABLE_LLM=1 and OPENAI_API_KEY is set.
    Returns a dict containing PEAK fields (B..U) + optional meta:
      - _ai_confidence (0..1)
      - _ai_notes (short)
    """
    if not _env_bool("ENABLE_LLM", default=False):
        return {}

    if not os.getenv("OPENAI_API_KEY"):
        return {}

    model = os.getenv("OPENAI_MODEL") or "gpt-4.1-mini"

    full_text = _normalize_text(text or "")
    t = _truncate_text_smart(full_text, int(os.getenv("OPENAI_TEXT_MAX", "22000") or "22000"))

    vendor_guess = _guess_vendor_code(full_text, hint=platform_hint)
    jp_guess, vr_guess = _guess_vat(full_text)
    pay_guess = _guess_payment_method(full_text)
    vendor_tax_guess = _guess_vendor_tax_id(full_text)

    schema = {
        "B_doc_date": "YYYYMMDD",
        "C_reference": "string <=64",
        "D_vendor_code": f"one of {sorted(VENDOR_CODES)}",
        "E_tax_id_13": "13 digits or empty",
        "F_branch_5": "5 digits (00000 allowed) or empty",
        "G_invoice_no": "string",
        "H_invoice_date": "YYYYMMDD",
        "I_tax_purchase_date": "YYYYMMDD",
        "J_price_type": "1|2|3",
        "K_account": "string (optional)",
        "L_description": "short description",
        "M_qty": "number-as-string (default 1)",
        "N_unit_price": "number-as-string",
        "O_vat_rate": "7%|NO",
        "P_wht": "0|number|percent (e.g. 3%)",
        "Q_payment_method": "string (optional)",
        "R_paid_amount": "number-as-string",
        "S_pnd": "1|2|3|53|empty",
        "T_note": "string",
        "U_group": f"one of {sorted(GROUPS)}",
        "_ai_confidence": "0..1",
        "_ai_notes": "short Thai explanation",
    }

    group_dictionary = (
        "Dictionary:\n"
        "- Lazada / Shopee / TikTok / SPX → Marketplace Expense\n"
        "- Advertising / Sponsored / Ads → Advertising Expense\n"
        "- Goods / Product / Inventory → Inventory/COGS\n"
        "Rule: If vendor tax id missing → mention 'e-Tax Ready = NO' in T_note.\n"
    )

    system = (
        "You are a meticulous Thai accounting document extraction engine. "
        "Extract structured fields for PEAK expense import. "
        "Return STRICT JSON ONLY. No markdown. No extra text. "
        "If unsure, leave empty but try your best. "
        "Normalize dates to YYYYMMDD (Gregorian). "
        "Tax ID must be 13 digits. Branch must be 5 digits (00000 allowed). "
        "price_type: 1=VAT separated, 2=VAT included, 3=no VAT. "
        "vat_rate: 7% or NO. "
        "Prefer total including VAT for R_paid_amount when present. "
        "Keep T_note clean (no error text)."
    )

    user_payload = {
        "source_file": source_filename,
        "platform_hint": platform_hint,
        "vendor_guess": vendor_guess,
        "vat_guess": {"J_price_type": jp_guess, "O_vat_rate": vr_guess},
        "payment_guess": pay_guess,
        "vendor_tax_id_guess": vendor_tax_guess,
        "partial_row_json": partial_row or {},
        "required_schema_keys": schema,
        "dictionary": group_dictionary,
        "document_text": t,
    }

    try:
        out = _openai_chat_json(system=system, user=json.dumps(user_payload, ensure_ascii=False), model=model)
    except Exception:
        return {}

    allowed = set(schema.keys())
    cleaned: Dict[str, Any] = {}
    for k, v in (out or {}).items():
        if k in allowed:
            cleaned[k] = v

    # -----------------------------
    # Post-normalization & guardrails
    # -----------------------------

    cleaned["D_vendor_code"] = _clamp_choice(cleaned.get("D_vendor_code"), VENDOR_CODES, vendor_guess)
    cleaned["J_price_type"] = _clamp_choice(str(cleaned.get("J_price_type", "")).strip(), PRICE_TYPES, jp_guess)
    cleaned["O_vat_rate"] = _clamp_choice(cleaned.get("O_vat_rate"), VAT_RATES, vr_guess)

    cleaned["E_tax_id_13"] = _to_tax13(cleaned.get("E_tax_id_13")) or vendor_tax_guess
    cleaned["F_branch_5"] = _to_branch5(cleaned.get("F_branch_5"))

    for dk in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        v = str(cleaned.get(dk, "") or "").strip()
        cleaned[dk] = v if re.fullmatch(r"\d{8}", v) else ""

    cleaned["M_qty"] = str(cleaned.get("M_qty") or "1").strip() or "1"
    cleaned["N_unit_price"] = _to_money_2(cleaned.get("N_unit_price")) or ""
    cleaned["R_paid_amount"] = _to_money_2(cleaned.get("R_paid_amount")) or ""

    if not cleaned["R_paid_amount"] and cleaned["N_unit_price"]:
        cleaned["R_paid_amount"] = cleaned["N_unit_price"]

    p_wht = cleaned.get("P_wht", "")
    if isinstance(p_wht, (int, float, Decimal)):
        cleaned["P_wht"] = _to_money_2(p_wht) or "0"
    else:
        s = str(p_wht or "").strip()
        if not s or s in {"0", "0.00"}:
            cleaned["P_wht"] = "0"
        elif "%" in s and cleaned["N_unit_price"]:
            amt = _wht_percent_to_amount(cleaned["N_unit_price"], s)
            cleaned["P_wht"] = amt or "0"
        else:
            cleaned["P_wht"] = _to_money_2(s) or "0"

    s_pnd = str(cleaned.get("S_pnd", "") or "").strip()
    if s_pnd not in PND_ALLOWED:
        s_pnd = ""
    if s_pnd == "" and cleaned.get("P_wht") not in ("", "0", "0.00"):
        s_pnd = _guess_pnd(full_text, cleaned["P_wht"])
    cleaned["S_pnd"] = s_pnd

    if not str(cleaned.get("Q_payment_method", "") or "").strip():
        cleaned["Q_payment_method"] = pay_guess

    ug = str(cleaned.get("U_group", "") or "").strip()
    if ug not in GROUPS:
        if cleaned["D_vendor_code"] in {"Shopee", "Lazada", "TikTok", "SPX"}:
            cleaned["U_group"] = "Marketplace Expense"
        else:
            cleaned["U_group"] = "Other Expense"

    if not str(cleaned.get("L_description", "") or "").strip():
        cleaned["L_description"] = cleaned["U_group"] or "Other Expense"

    note = str(cleaned.get("T_note", "") or "").strip()
    note_lines = [ln.strip() for ln in note.splitlines() if ln.strip()]

    if not cleaned.get("E_tax_id_13"):
        note_lines.append("e-Tax Ready = NO (ไม่พบเลขประจำตัวผู้เสียภาษีผู้ขาย)")
    if source_filename:
        note_lines.append(f"Source File: {source_filename}")
    if cleaned.get("D_vendor_code"):
        note_lines.append(f"Platform: {cleaned['D_vendor_code']}")

    # de-dup while preserving order
    deduped: list[str] = []
    seen = set()
    for ln in note_lines:
        if ln not in seen:
            seen.add(ln)
            deduped.append(ln)

    cleaned["T_note"] = "\n".join(deduped)[:1500]

    try:
        c = float(cleaned.get("_ai_confidence", 0))
        cleaned["_ai_confidence"] = max(0.0, min(1.0, c))
    except Exception:
        cleaned["_ai_confidence"] = 0.0

    final: Dict[str, Any] = {}
    for k in schema.keys():
        if k in cleaned:
            final[k] = cleaned[k]

    return final


__all__ = ["ai_fill_peak_row"]
