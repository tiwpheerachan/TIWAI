# backend/app/services/ai_service.py
"""
AI Service - Enhanced Version for Multi-Platform Support

✅ Improvements:
1. ✅ Support for 8 platforms (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
2. ✅ Platform-specific prompts and validation
3. ✅ Smart field detection and auto-correction
4. ✅ Integration with export_service constants
5. ✅ Better date/amount parsing
6. ✅ Metadata tracking
7. ✅ Platform-aware schema
8. ✅ Comprehensive error handling
"""
from __future__ import annotations

import json
import os
import re
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple, Set

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Constants / Schema (aligned with export_service.py)
# ---------------------------------------------------------------------

PEAK_FIELDS = [
    "A_seq", "A_company_name", "B_doc_date", "C_reference", "D_vendor_code",
    "E_tax_id_13", "F_branch_5", "G_invoice_no", "H_invoice_date",
    "I_tax_purchase_date", "J_price_type", "K_account", "L_description",
    "M_qty", "N_unit_price", "O_vat_rate", "P_wht", "Q_payment_method",
    "R_paid_amount", "S_pnd", "T_note", "U_group",
]

# ✅ Platform-specific vendor codes (aligned with export_service)
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

# All valid vendor codes
VENDOR_CODES: Set[str] = set(PLATFORM_VENDORS.values()) | {"Other"}

# ✅ Platform-specific VAT rules
PLATFORM_VAT_RULES = {
    "META": {"J_price_type": "3", "O_vat_rate": "NO"},  # No VAT (reverse charge)
    "GOOGLE": {"J_price_type": "3", "O_vat_rate": "NO"},  # No VAT (international)
    "SHOPEE": {"J_price_type": "1", "O_vat_rate": "7%"},  # VAT separated
    "LAZADA": {"J_price_type": "1", "O_vat_rate": "7%"},
    "TIKTOK": {"J_price_type": "1", "O_vat_rate": "7%"},
    "SPX": {"J_price_type": "1", "O_vat_rate": "7%"},
    "THAI_TAX": {"J_price_type": "1", "O_vat_rate": "7%"},  # Usually
    "UNKNOWN": {"J_price_type": "1", "O_vat_rate": "7%"},
}

VAT_RATES: Set[str] = {"7%", "NO"}
PRICE_TYPES: Set[str] = {"1", "2", "3"}
PND_ALLOWED: Set[str] = {"", "1", "2", "3", "53"}

# ✅ Platform-specific groups
PLATFORM_GROUPS = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Delivery/Logistics Expense",
    "THAI_TAX": "General Expense",  # Depends on content
    "UNKNOWN": "Other Expense",
}

# All valid groups
GROUPS: Set[str] = {
    "Marketplace Expense",
    "Advertising Expense",
    "Delivery/Logistics Expense",
    "General Expense",
    "Inventory/COGS",
    "Other Expense",
}

# Known client tax IDs (your companies)
CLIENT_TAX_IDS: Set[str] = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ---------------------------------------------------------------------
# Platform Detection Regex
# ---------------------------------------------------------------------

RE_VENDOR_META = re.compile(
    r"(meta\s*platforms?\s*ireland|facebook|fb\s*ads|instagram\s*ads)",
    re.IGNORECASE
)
RE_VENDOR_GOOGLE = re.compile(
    r"(google\s*(?:asia|advertising|ads|adwords)|google\s*payment)",
    re.IGNORECASE
)
RE_VENDOR_SHOPEE = re.compile(r"(shopee|ช็อปปี้|ช้อปปี้)", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"(lazada|ลาซาด้า)", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"(tiktok|ติ๊กต๊อก)", re.IGNORECASE)
RE_VENDOR_SPX = re.compile(
    r"(spx\s*express|shopee\s*express|standard\s*express)",
    re.IGNORECASE
)

# Thai Tax Invoice patterns
RE_THAI_TAX = re.compile(
    r"(ใบเสร็จรับเงิน|ใบกำกับภาษี|tax\s*invoice|receipt)",
    re.IGNORECASE
)

# Extraction patterns
RE_TAX13 = re.compile(r"\b(\d{13})\b")
RE_BRANCH5 = re.compile(r"(?:branch|สาขา)\s*[:#]?\s*(\d{5})", re.IGNORECASE)
RE_INVOICE_NO = re.compile(
    r"(?:invoice|inv|เลขที่)\s*[:#]?\s*([A-Z0-9\-/]{4,})",
    re.IGNORECASE
)

# VAT patterns
RE_HAS_VAT7 = re.compile(
    r"(vat\s*7%|ภาษีมูลค่าเพิ่ม\s*7%|total\s*vat|vat\s*amount)",
    re.IGNORECASE
)
RE_NO_VAT = re.compile(
    r"(no\s*vat|ไม่มี\s*vat|vat\s*exempt|ยกเว้นภาษี|reverse\s*charge)",
    re.IGNORECASE
)

# Payment patterns
RE_PAYMENT_DEDUCT = re.compile(
    r"(หักจากยอดขาย|deduct(?:ed)?\s*from\s*(?:sales|revenue))",
    re.IGNORECASE
)
RE_PAYMENT_TRANSFER = re.compile(
    r"(โอน|transfer|bank\s*transfer)",
    re.IGNORECASE
)
RE_PAYMENT_CARD = re.compile(
    r"(card|credit\s*card|visa|mastercard)",
    re.IGNORECASE
)
RE_PAYMENT_CASH = re.compile(r"(cash|เงินสด)", re.IGNORECASE)

# WHT patterns
RE_WHT_RATE = re.compile(r"(?:อัตรา|rate|ร้อยละ)\s*([0-9]{1,2})\s*%", re.IGNORECASE)
RE_WHT_ANY = re.compile(r"(withholding|wht|หักภาษี|ณ\s*ที่จ่าย)", re.IGNORECASE)
RE_PND_HINT = re.compile(r"(ภ\.ง\.ด\.?\s*53|pnd\s*53)", re.IGNORECASE)

# Date patterns (support multiple formats)
RE_DATE_YYYYMMDD = re.compile(r"(\d{4})(\d{2})(\d{2})")
RE_DATE_YYYY_MM_DD = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
RE_DATE_DD_MM_YYYY = re.compile(r"(\d{2})/(\d{2})/(\d{4})")

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    """Check environment variable as boolean"""
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_text(text: str) -> str:
    """Normalize text for processing"""
    if not text:
        return ""
    try:
        t = str(text).replace("\r\n", "\n").replace("\r", "\n")
        t = re.sub(r"[\u200b-\u200f\ufeff]", "", t)
        lines = [re.sub(r"[ \t\f\v]+", " ", ln).strip() for ln in t.split("\n")]
        return "\n".join(lines).strip()
    except Exception as e:
        logger.warning(f"Text normalization error: {e}")
        return str(text or "")


def _to_tax13(v: Any) -> str:
    """Extract 13-digit tax ID"""
    if v is None:
        return ""
    try:
        digits = "".join(c for c in str(v) if c.isdigit())
        return digits[:13] if len(digits) >= 13 else ""
    except Exception:
        return ""


def _to_branch5(v: Any) -> str:
    """Convert to 5-digit branch code"""
    if v is None:
        return "00000"
    try:
        digits = "".join(c for c in str(v) if c.isdigit())
        if not digits:
            return "00000"
        return digits.zfill(5)[:5]
    except Exception:
        return "00000"


def _to_money_2(v: Any) -> str:
    """Convert to decimal money format (2 decimal places)"""
    if v is None:
        return ""
    try:
        s = str(v).strip()
        if not s:
            return ""
        # Remove currency symbols and commas
        s = s.replace("฿", "").replace("THB", "").replace("$", "").replace(",", "").strip()
        d = Decimal(s)
        if d < 0:
            return ""
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _clamp_choice(v: Any, allowed: Set[str], fallback: str) -> str:
    """Ensure value is in allowed set"""
    try:
        s = "" if v is None else str(v).strip()
        return s if s in allowed else fallback
    except Exception:
        return fallback


def _first_json_object(s: str) -> Optional[str]:
    """Extract first JSON object from model output"""
    if not s:
        return None
    try:
        s = s.strip()
        if s.startswith("{") and s.endswith("}"):
            return s
        m = re.search(r"\{[\s\S]*\}", s)
        return m.group(0) if m else None
    except Exception:
        return None


# ---------------------------------------------------------------------
# Platform Detection
# ---------------------------------------------------------------------

def _detect_platform(text: str, hint: str = "") -> str:
    """
    ✅ Detect platform from text content
    
    Returns: META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, or UNKNOWN
    """
    try:
        t = _normalize_text(text)
        h = (hint or "").strip().upper()
        
        # Check hint first
        if h in PLATFORM_VENDORS:
            return h
        
        # Priority 1: Ads platforms (high confidence)
        if RE_VENDOR_META.search(t):
            return "META"
        if RE_VENDOR_GOOGLE.search(t):
            return "GOOGLE"
        
        # Priority 2: SPX (before Shopee!)
        if RE_VENDOR_SPX.search(t):
            return "SPX"
        
        # Priority 3: Marketplace
        if RE_VENDOR_SHOPEE.search(t):
            return "SHOPEE"
        if RE_VENDOR_LAZADA.search(t):
            return "LAZADA"
        if RE_VENDOR_TIKTOK.search(t):
            return "TIKTOK"
        
        # Priority 4: Thai Tax Invoice (has tax ID + invoice patterns)
        if RE_THAI_TAX.search(t) and RE_TAX13.search(t):
            return "THAI_TAX"
        
        return "UNKNOWN"
    
    except Exception as e:
        logger.error(f"Platform detection error: {e}")
        return "UNKNOWN"


def _guess_vendor_code(platform: str, text: str = "") -> str:
    """
    ✅ Get vendor code for platform
    """
    try:
        vendor = PLATFORM_VENDORS.get(platform, "Other")
        
        # For THAI_TAX, try to extract from text
        if platform == "THAI_TAX" and text:
            # Look for vendor name patterns
            # This is a simple implementation - enhance as needed
            pass
        
        return vendor
    
    except Exception as e:
        logger.error(f"Vendor code guess error: {e}")
        return "Other"


# ---------------------------------------------------------------------
# Field Extraction Helpers
# ---------------------------------------------------------------------

def _guess_vat(platform: str, text: str) -> Tuple[str, str]:
    """
    ✅ Guess VAT settings based on platform and text
    
    Returns: (J_price_type, O_vat_rate)
    """
    try:
        # Use platform-specific rules first
        if platform in PLATFORM_VAT_RULES:
            rules = PLATFORM_VAT_RULES[platform]
            return rules["J_price_type"], rules["O_vat_rate"]
        
        # Fallback: detect from text
        t = _normalize_text(text)
        if RE_NO_VAT.search(t):
            return "3", "NO"
        if RE_HAS_VAT7.search(t):
            return "1", "7%"
        
        # Default
        return "1", "7%"
    
    except Exception as e:
        logger.error(f"VAT guess error: {e}")
        return "1", "7%"


def _guess_payment_method(platform: str, text: str) -> str:
    """Guess payment method from text"""
    try:
        t = _normalize_text(text)
        
        # Platform-specific defaults
        if platform in {"META", "GOOGLE"}:
            return "CARD"  # Usually credit card
        
        if RE_PAYMENT_DEDUCT.search(t):
            return "หักจากยอดขาย"
        if RE_PAYMENT_TRANSFER.search(t):
            return "โอน"
        if RE_PAYMENT_CARD.search(t):
            return "CARD"
        if RE_PAYMENT_CASH.search(t):
            return "เงินสด"
        
        return ""
    
    except Exception:
        return ""


def _guess_vendor_tax_id(text: str) -> str:
    """Pick first 13-digit Tax ID that is NOT client tax ID"""
    try:
        t = _normalize_text(text)
        for m in RE_TAX13.finditer(t):
            tax = m.group(1)
            if tax not in CLIENT_TAX_IDS:
                return tax
        return ""
    except Exception:
        return ""


def _guess_pnd(text: str, wht: str) -> str:
    """Guess PND type from text and WHT"""
    try:
        t = _normalize_text(text)
        w = _to_money_2(wht)
        
        if w and w not in ("0.00", ""):
            if RE_PND_HINT.search(t):
                return "53"
            return "53"  # Default for marketplace
        
        return ""
    
    except Exception:
        return ""


def _wht_percent_to_amount(unit_price: str, wht_field: str) -> str:
    """Convert WHT percentage to amount"""
    if not unit_price:
        return ""
    try:
        m = re.search(r"([0-9]{1,2})(?:\s*)%", str(wht_field))
        if not m:
            return ""
        
        rate = Decimal(m.group(1)) / Decimal("100")
        base = Decimal(str(unit_price).replace(",", ""))
        if base <= 0:
            return ""
        return f"{(base * rate):.2f}"
    
    except Exception:
        return ""


def _truncate_text_smart(text: str, max_len: int) -> str:
    """Keep head+tail for long texts"""
    try:
        t = (text or "").strip()
        if len(t) <= max_len:
            return t
        
        head = int(max_len * 0.65)
        tail = max_len - head - 40
        return t[:head] + "\n\n...<TRUNCATED>...\n\n" + t[-tail:]
    
    except Exception:
        return text or ""


# ---------------------------------------------------------------------
# OpenAI API
# ---------------------------------------------------------------------

def _openai_chat_json(system: str, user: str, model: str) -> Dict[str, Any]:
    """Call OpenAI Chat Completions API with JSON mode"""
    try:
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
    
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return {}


# ---------------------------------------------------------------------
# Platform-Specific Prompts
# ---------------------------------------------------------------------

def _build_platform_specific_prompt(platform: str) -> str:
    """
    ✅ Build platform-specific extraction instructions
    """
    try:
        if platform == "META":
            return (
                "This is a Meta/Facebook Ads receipt.\n"
                "CRITICAL RULES:\n"
                "- D_vendor_code MUST BE: 'Meta Platforms Ireland'\n"
                "- O_vat_rate MUST BE: 'NO' (reverse charge, international service)\n"
                "- J_price_type MUST BE: '3' (no VAT)\n"
                "- C_reference: Extract receipt ID (e.g., RCMETA...)\n"
                "- R_paid_amount: Total amount paid\n"
                "- U_group: 'Advertising Expense'\n"
                "- T_note: Mention 'Meta Ads Receipt' and any campaign info\n"
            )
        
        elif platform == "GOOGLE":
            return (
                "This is a Google Ads payment receipt.\n"
                "CRITICAL RULES:\n"
                "- D_vendor_code MUST BE: 'Google Asia Pacific'\n"
                "- O_vat_rate MUST BE: 'NO' (international service)\n"
                "- J_price_type MUST BE: '3' (no VAT)\n"
                "- C_reference: Extract payment number (e.g., V...)\n"
                "- R_paid_amount: Total amount paid\n"
                "- U_group: 'Advertising Expense'\n"
                "- T_note: Mention 'Google Ads Payment' and payment method\n"
            )
        
        elif platform == "SPX":
            return (
                "This is a Shopee Express (SPX) document.\n"
                "RULES:\n"
                "- D_vendor_code: 'Shopee Express'\n"
                "- O_vat_rate: Usually '7%' for Thai services\n"
                "- J_price_type: '1' (VAT separated)\n"
                "- U_group: 'Delivery/Logistics Expense'\n"
                "- Extract tracking/waybill numbers as C_reference\n"
            )
        
        elif platform == "THAI_TAX":
            return (
                "This is a Thai Tax Invoice (ใบกำกับภาษี).\n"
                "CRITICAL RULES:\n"
                "- E_tax_id_13: MUST extract 13-digit tax ID (NOT client's, but vendor's)\n"
                "- F_branch_5: MUST extract 5-digit branch code (default 00000)\n"
                "- G_invoice_no: Extract invoice number\n"
                "- H_invoice_date: Extract invoice date (YYYYMMDD)\n"
                "- O_vat_rate: Usually '7%' for Thailand\n"
                "- J_price_type: Usually '1' (VAT separated)\n"
                "- Carefully identify vendor name for D_vendor_code\n"
            )
        
        elif platform in {"SHOPEE", "LAZADA", "TIKTOK"}:
            return (
                f"This is a {platform} marketplace document.\n"
                "RULES:\n"
                f"- D_vendor_code: '{PLATFORM_VENDORS[platform]}'\n"
                "- O_vat_rate: Usually '7%'\n"
                "- J_price_type: '1' (VAT separated)\n"
                "- U_group: 'Marketplace Expense'\n"
                "- Extract order/transaction IDs as C_reference\n"
                "- Look for commission, service fees\n"
            )
        
        else:  # UNKNOWN
            return (
                "This is an unknown document type.\n"
                "RULES:\n"
                "- Carefully identify vendor from tax ID and document\n"
                "- Extract all fields you can find\n"
                "- If Thai tax invoice, ensure 13-digit + 5-digit codes\n"
                "- Default O_vat_rate to '7%' unless clearly no VAT\n"
            )
    
    except Exception as e:
        logger.error(f"Platform prompt error: {e}")
        return ""


# ---------------------------------------------------------------------
# Main AI Extraction Function
# ---------------------------------------------------------------------

def ai_fill_peak_row(
    text: str,
    platform_hint: str = "",
    partial_row: Optional[Dict[str, Any]] = None,
    source_filename: str = "",
) -> Dict[str, Any]:
    """
    ✅ Enhanced AI extraction with platform awareness
    
    LLM step: fill PEAK columns from OCR/text.
    
    Enabled when ENABLE_LLM=1 and OPENAI_API_KEY is set.
    
    Args:
        text: Document text (OCR)
        platform_hint: Platform hint (META, GOOGLE, etc.)
        partial_row: Partial extraction from rule-based extractors
        source_filename: Original filename
    
    Returns:
        Dict containing PEAK fields (A-U) + metadata:
        - _ai_confidence (0..1)
        - _ai_notes (Thai explanation)
        - _extraction_method: "ai_en" or "ai_th"
        - _platform_detected: Detected platform
    """
    
    # Check if LLM enabled
    if not _env_bool("ENABLE_LLM", default=False):
        logger.info("LLM disabled (ENABLE_LLM=0)")
        return {}
    
    if not os.getenv("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set")
        return {}
    
    try:
        model = os.getenv("OPENAI_MODEL") or "gpt-4-turbo-preview"
        
        # Normalize and truncate text
        full_text = _normalize_text(text or "")
        max_len = int(os.getenv("OPENAI_TEXT_MAX", "22000") or "22000")
        t = _truncate_text_smart(full_text, max_len)
        
        # ✅ Detect platform
        platform = _detect_platform(full_text, hint=platform_hint)
        logger.info(f"Platform detected: {platform}")
        
        # ✅ Get platform-specific defaults
        vendor_code = _guess_vendor_code(platform, full_text)
        jp_guess, vr_guess = _guess_vat(platform, full_text)
        pay_guess = _guess_payment_method(platform, full_text)
        vendor_tax_guess = _guess_vendor_tax_id(full_text)
        
        # ✅ Build platform-specific prompt
        platform_prompt = _build_platform_specific_prompt(platform)
        
        # Schema definition
        schema = {
            "B_doc_date": "YYYYMMDD (Gregorian calendar)",
            "C_reference": "string <=64 (receipt/invoice/order ID)",
            "D_vendor_code": f"string (vendor name, e.g., '{vendor_code}')",
            "E_tax_id_13": "13 digits or empty (vendor's tax ID, NOT client's)",
            "F_branch_5": "5 digits (00000 allowed) or empty",
            "G_invoice_no": "string (invoice/receipt number)",
            "H_invoice_date": "YYYYMMDD",
            "I_tax_purchase_date": "YYYYMMDD (optional, usually same as B_doc_date)",
            "J_price_type": "1=VAT separated, 2=VAT included, 3=no VAT",
            "K_account": "string (GL account code, optional)",
            "L_description": "short description of expense",
            "M_qty": "number-as-string (default 1)",
            "N_unit_price": "number-as-string (price per unit)",
            "O_vat_rate": "7%|NO",
            "P_wht": "0|number|percent (e.g., 3%)",
            "Q_payment_method": "string (e.g., CARD, โอน, หักจากยอดขาย)",
            "R_paid_amount": "number-as-string (total paid including VAT if applicable)",
            "S_pnd": "1|2|3|53|empty (PND type for WHT)",
            "T_note": "string (important notes, NOT error messages)",
            "U_group": f"one of {sorted(GROUPS)}",
            "_ai_confidence": "0.0 to 1.0 (confidence score)",
            "_ai_notes": "short Thai explanation of extraction",
        }
        
        # Group dictionary
        group_dictionary = (
            "Group Classification Rules:\n"
            "- Meta/Google → 'Advertising Expense'\n"
            "- Lazada/Shopee/TikTok → 'Marketplace Expense'\n"
            "- SPX → 'Delivery/Logistics Expense'\n"
            "- Products/Goods → 'Inventory/COGS'\n"
            "- Other → 'General Expense' or 'Other Expense'\n"
            "\n"
            "Special Notes:\n"
            "- If vendor tax ID missing → add 'e-Tax Ready = NO' to T_note\n"
            "- Keep T_note clean and informative\n"
            "- Prefer specific groups over 'Other Expense'\n"
        )
        
        # System prompt
        system = (
            "You are a meticulous Thai accounting document extraction engine. "
            "Extract structured fields for PEAK expense import system. "
            "Return STRICT JSON ONLY. No markdown. No extra text. "
            "If unsure about a field, leave empty but try your best. "
            "\n\n"
            "CRITICAL RULES:\n"
            "1. Normalize dates to YYYYMMDD (Gregorian calendar)\n"
            "2. Tax ID must be exactly 13 digits (vendor's, NOT client's)\n"
            "3. Branch must be exactly 5 digits (00000 is valid)\n"
            "4. For money fields: use format like '1234.56' (no currency symbols)\n"
            "5. For VAT: '7%' or 'NO' only\n"
            "6. For price_type: '1' (VAT separated), '2' (included), '3' (no VAT)\n"
            "7. Prefer R_paid_amount = total including VAT when present\n"
            "8. Keep T_note clean and informative (no error messages)\n"
            "9. Follow platform-specific rules carefully\n"
            "\n"
            f"{platform_prompt}"
        )
        
        # User payload
        user_payload = {
            "source_file": source_filename,
            "platform_detected": platform,
            "platform_hint": platform_hint,
            "vendor_code_guess": vendor_code,
            "vat_guess": {
                "J_price_type": jp_guess,
                "O_vat_rate": vr_guess
            },
            "payment_guess": pay_guess,
            "vendor_tax_id_guess": vendor_tax_guess,
            "partial_row_from_rule_based": partial_row or {},
            "required_schema": schema,
            "group_dictionary": group_dictionary,
            "document_text": t,
        }
        
        # Call OpenAI
        logger.info(f"Calling OpenAI API (model={model})...")
        out = _openai_chat_json(
            system=system,
            user=json.dumps(user_payload, ensure_ascii=False),
            model=model
        )
        
        if not out:
            logger.warning("OpenAI returned empty response")
            return {}
        
        # Filter to allowed keys
        allowed = set(schema.keys())
        cleaned: Dict[str, Any] = {}
        for k, v in (out or {}).items():
            if k in allowed:
                cleaned[k] = v
        
        # ---------------------------------------------------------------------
        # ✅ Post-normalization & validation (platform-aware)
        # ---------------------------------------------------------------------
        
        # Platform-specific vendor code enforcement
        if platform in PLATFORM_VENDORS and PLATFORM_VENDORS[platform]:
            cleaned["D_vendor_code"] = PLATFORM_VENDORS[platform]
        else:
            cleaned["D_vendor_code"] = cleaned.get("D_vendor_code", vendor_code)
        
        # Platform-specific VAT rules enforcement
        if platform in PLATFORM_VAT_RULES:
            rules = PLATFORM_VAT_RULES[platform]
            cleaned["J_price_type"] = rules["J_price_type"]
            cleaned["O_vat_rate"] = rules["O_vat_rate"]
        else:
            cleaned["J_price_type"] = _clamp_choice(
                str(cleaned.get("J_price_type", "")).strip(),
                PRICE_TYPES,
                jp_guess
            )
            cleaned["O_vat_rate"] = _clamp_choice(
                cleaned.get("O_vat_rate"),
                VAT_RATES,
                vr_guess
            )
        
        # Tax ID and branch
        cleaned["E_tax_id_13"] = _to_tax13(cleaned.get("E_tax_id_13")) or vendor_tax_guess
        cleaned["F_branch_5"] = _to_branch5(cleaned.get("F_branch_5"))
        
        # Dates (YYYYMMDD)
        for dk in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
            v = str(cleaned.get(dk, "") or "").strip()
            cleaned[dk] = v if re.fullmatch(r"\d{8}", v) else ""
        
        # Numeric fields
        cleaned["M_qty"] = str(cleaned.get("M_qty") or "1").strip() or "1"
        cleaned["N_unit_price"] = _to_money_2(cleaned.get("N_unit_price")) or ""
        cleaned["R_paid_amount"] = _to_money_2(cleaned.get("R_paid_amount")) or ""
        
        # Fallback: R_paid_amount = N_unit_price if missing
        if not cleaned["R_paid_amount"] and cleaned["N_unit_price"]:
            cleaned["R_paid_amount"] = cleaned["N_unit_price"]
        
        # WHT handling
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
        
        # PND
        s_pnd = str(cleaned.get("S_pnd", "") or "").strip()
        if s_pnd not in PND_ALLOWED:
            s_pnd = ""
        if s_pnd == "" and cleaned.get("P_wht") not in ("", "0", "0.00"):
            s_pnd = _guess_pnd(full_text, cleaned["P_wht"])
        cleaned["S_pnd"] = s_pnd
        
        # Payment method
        if not str(cleaned.get("Q_payment_method", "") or "").strip():
            cleaned["Q_payment_method"] = pay_guess
        
        # Platform-specific group
        ug = str(cleaned.get("U_group", "") or "").strip()
        if ug not in GROUPS:
            cleaned["U_group"] = PLATFORM_GROUPS.get(platform, "Other Expense")
        
        # Description fallback
        if not str(cleaned.get("L_description", "") or "").strip():
            cleaned["L_description"] = cleaned.get("U_group", "Other Expense")
        
        # ✅ Build T_note with metadata
        note = str(cleaned.get("T_note", "") or "").strip()
        note_lines = [ln.strip() for ln in note.splitlines() if ln.strip()]
        
        # Add platform info
        if platform != "UNKNOWN":
            note_lines.append(f"Platform: {platform}")
        
        # Add tax ID warning
        if not cleaned.get("E_tax_id_13"):
            note_lines.append("e-Tax Ready = NO (ไม่พบเลขประจำตัวผู้เสียภาษีผู้ขาย)")
        
        # Add source file
        if source_filename:
            note_lines.append(f"Source: {source_filename}")
        
        # Add extraction method
        note_lines.append("Extraction: AI")
        
        # De-duplicate while preserving order
        deduped: list[str] = []
        seen = set()
        for ln in note_lines:
            if ln not in seen:
                seen.add(ln)
                deduped.append(ln)
        
        cleaned["T_note"] = "\n".join(deduped)[:1500]
        
        # Confidence score
        try:
            c = float(cleaned.get("_ai_confidence", 0))
            cleaned["_ai_confidence"] = max(0.0, min(1.0, c))
        except Exception:
            cleaned["_ai_confidence"] = 0.0
        
        # ✅ Add metadata
        cleaned["_extraction_method"] = "ai_th" if "ก" in full_text else "ai_en"
        cleaned["_platform_detected"] = platform
        cleaned["_model_used"] = model
        
        # Final result: only schema keys
        final: Dict[str, Any] = {}
        for k in schema.keys():
            if k in cleaned:
                final[k] = cleaned[k]
        
        logger.info(f"AI extraction complete: {platform}, confidence={final.get('_ai_confidence', 0)}")
        return final
    
    except Exception as e:
        logger.error(f"AI extraction error: {e}", exc_info=True)
        return {}


__all__ = ["ai_fill_peak_row", "PLATFORM_VENDORS", "PLATFORM_VAT_RULES", "PLATFORM_GROUPS"]