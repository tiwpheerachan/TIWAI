"""
Vendor Code Mapping System (v3.3) — PEAK Importer

Source of Truth:
  client_tax_id (บริษัทเรา เช่น Rabbit/SHD/TopOne)
  vendor_tax_id (ผู้รับเงิน เช่น Shopee/SPX/Lazada/TikTok/Shopify/Marketplace)
    -> vendor_code (Cxxxxx)

Goals:
✅ get_vendor_code() คืน "Cxxxxx" เสมอเมื่อรู้ client + vendor (ไม่ว่า caller จะส่ง vendor_tax_id / vendor_name ผิดรูปแบบ)
✅ fallback ห้ามคืนชื่อ platform (กัน D_vendor_code หลุดเป็น Shopee/Lazada)
✅ normalize tax id / name ให้ robust (รวมกรณีมีข้อความปน, OCR, Thai digit)
✅ get_expense_category() ตาม rules
"""

from __future__ import annotations

from typing import Dict, Optional
import re

# ============================================================
# Client Tax ID Constants
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD    = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Vendor Tax ID Constants (canonical)
# ============================================================
VENDOR_SHOPEE             = "0105558019581"   # Shopee (Thailand) Co., Ltd.
VENDOR_LAZADA             = "010556214176"    # Lazada E-Services (Thailand) Co., Ltd.
VENDOR_TIKTOK             = "0105555040244"   # TikTok
VENDOR_MARKETPLACE_OTHER  = "0105548000241"   # Marketplace/ตัวกลาง
VENDOR_SHOPIFY            = "0993000475879"   # Shopify Commerce Singapore
VENDOR_SPX                = "0105561164871"   # SPX Express (Thailand)

# ============================================================
# Source of Truth: Nested dict mapping
# client_tax_id -> vendor_tax_id -> vendor_code (Cxxxxx)
# ============================================================
VENDOR_CODE_BY_CLIENT: Dict[str, Dict[str, str]] = {
    CLIENT_RABBIT: {
        VENDOR_SHOPEE: "C00395",
        VENDOR_LAZADA: "C00411",
        VENDOR_TIKTOK: "C00562",
        VENDOR_MARKETPLACE_OTHER: "C01031",
        VENDOR_SHOPIFY: "C01143",
        VENDOR_SPX: "C00563",
    },
    CLIENT_SHD: {
        VENDOR_SHOPEE: "C00888",
        VENDOR_LAZADA: "C01132",
        VENDOR_TIKTOK: "C01246",
        VENDOR_MARKETPLACE_OTHER: "C01420",
        VENDOR_SHOPIFY: "C33491",
        VENDOR_SPX: "C01133",
    },
    CLIENT_TOPONE: {
        VENDOR_SHOPEE: "C00020",
        VENDOR_LAZADA: "C00025",
        VENDOR_TIKTOK: "C00051",
        VENDOR_MARKETPLACE_OTHER: "C00095",
        VENDOR_SPX: "C00038",
        # VENDOR_SHOPIFY: "Cxxxxx",
    },
}

# ============================================================
# Vendor Name -> Vendor Tax ID mapping (fallback by name)
# ============================================================
VENDOR_NAME_TO_TAX: Dict[str, str] = {
    # Shopee
    "shopee": VENDOR_SHOPEE,
    "ช้อปปี้": VENDOR_SHOPEE,
    "shopee (thailand)": VENDOR_SHOPEE,
    "shopee thailand": VENDOR_SHOPEE,

    # Lazada
    "lazada": VENDOR_LAZADA,
    "ลาซาด้า": VENDOR_LAZADA,
    "lazada e-services": VENDOR_LAZADA,
    "lazada e services": VENDOR_LAZADA,

    # TikTok
    "tiktok": VENDOR_TIKTOK,
    "ติ๊กต๊อก": VENDOR_TIKTOK,
    "tiktok shop": VENDOR_TIKTOK,

    # SPX
    "spx": VENDOR_SPX,
    "spx express": VENDOR_SPX,

    # Shopify
    "shopify": VENDOR_SHOPIFY,
    "shopify commerce": VENDOR_SHOPIFY,

    # Marketplace / other
    "marketplace": VENDOR_MARKETPLACE_OTHER,
    "ตัวกลาง": VENDOR_MARKETPLACE_OTHER,
    "มาร์เก็ตเพลส": VENDOR_MARKETPLACE_OTHER,
    "better marketplace": VENDOR_MARKETPLACE_OTHER,
    "เบ็ตเตอร์": VENDOR_MARKETPLACE_OTHER,
}

# ============================================================
# Aliases for vendor tax IDs (OCR mistakes / variant formats)
# map alias -> canonical vendor tax id
# ============================================================
ALIAS_VENDOR_TAX_ID_MAP: Dict[str, str] = {
    # ตัวอย่าง: OCR สลับ I/1, O/0 (เติมเองตามที่เจอ)
    # "010555801958I": VENDOR_SHOPEE,
}

# ============================================================
# Normalization helpers
# ============================================================
_TAX13_RE = re.compile(r"\b\d{13}\b")
_CCODE_RE = re.compile(r"^C\d{5}$", re.IGNORECASE)

def _thai_digits_to_arabic(s: str) -> str:
    # เผื่อ OCR ไทย: ๐๑๒๓๔๕๖๗๘๙
    th = "๐๑๒๓๔๕๖๗๘๙"
    ar = "0123456789"
    trans = str.maketrans({th[i]: ar[i] for i in range(10)})
    return s.translate(trans)

def _norm_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = _thai_digits_to_arabic(s)
    s = re.sub(r"\s+", " ", s)
    return s

def _extract_13_digits(s: str) -> str:
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    m = _TAX13_RE.search(s)
    return m.group(0) if m else ""

def _norm_tax_id(tax_id: str) -> str:
    """
    Normalize tax id:
    - if embedded 13 digits -> take it
    - if not 13 digits -> return "" (caller should treat as "name", not id)
    - apply alias map
    """
    s = (tax_id or "").strip()
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)

    d13 = _extract_13_digits(s)
    if not d13:
        return ""  # สำคัญ: ถ้าไม่ใช่ 13 หลัก อย่าแกล้งคืน string เดิม
    return ALIAS_VENDOR_TAX_ID_MAP.get(d13, d13)

def _is_known_client(client_tax_id: str) -> bool:
    c = _norm_tax_id(client_tax_id)
    return c in (CLIENT_RABBIT, CLIENT_SHD, CLIENT_TOPONE)

def _code_is_valid(code: str) -> bool:
    return bool(code) and bool(_CCODE_RE.match(code.strip()))

# ============================================================
# Public API: resolve vendor tax id from name
# ============================================================
def get_vendor_tax_id_from_name(vendor_name: str) -> str:
    """
    Best-effort resolve vendor_tax_id from vendor_name/platform string.
    """
    vn = _norm_name(vendor_name)
    if not vn:
        return ""
    for key, tax in VENDOR_NAME_TO_TAX.items():
        if key in vn:
            return tax
    return ""

# ============================================================
# Public API: main mapping function (HARDENED)
# ============================================================
def get_vendor_code(client_tax_id: str, vendor_tax_id: str = "", vendor_name: str = "") -> str:
    """
    Return vendor code (Cxxxxx) for PEAK "ผู้รับเงิน/คู่ค้า".

    Hardened behavior:
    - vendor_tax_id ถ้า caller ส่ง "Shopee" มา จะถูกมองว่าเป็นชื่อ ไม่ใช่ tax id
    - ถ้ารู้ client + vendor (จาก tax หรือ name) ต้องคืน Cxxxxx เสมอ
    - ห้ามคืนชื่อ platform เด็ดขาด
    """
    c = _norm_tax_id(client_tax_id)

    # client unknown -> Unknown
    if not c or not _is_known_client(c):
        return "Unknown"

    # 1) try vendor tax id (if truly 13 digits)
    v = _norm_tax_id(vendor_tax_id)
    if v:
        code = VENDOR_CODE_BY_CLIENT.get(c, {}).get(v)
        if _code_is_valid(code or ""):
            return code

    # 2) if vendor_tax_id isn't 13 digits, treat it as name hint too
    name_hint = vendor_name or vendor_tax_id or ""
    v2 = get_vendor_tax_id_from_name(name_hint)
    if v2:
        code = VENDOR_CODE_BY_CLIENT.get(c, {}).get(v2)
        if _code_is_valid(code or ""):
            return code

    # 3) strict fallback
    return "Unknown"

# ============================================================
# Optional: detect client by context (best effort)
# ============================================================
def detect_client_from_context(text: str) -> Optional[str]:
    t = _norm_name(text)
    if CLIENT_RABBIT in t or "rabbit" in t:
        return CLIENT_RABBIT
    if CLIENT_SHD in t or "shd" in t:
        return CLIENT_SHD
    if CLIENT_TOPONE in t or "topone" in t or "top one" in t:
        return CLIENT_TOPONE
    return None

def get_client_name(client_tax_id: str) -> str:
    c = _norm_tax_id(client_tax_id)
    if c == CLIENT_RABBIT:
        return "Rabbit"
    if c == CLIENT_SHD:
        return "SHD"
    if c == CLIENT_TOPONE:
        return "TopOne"
    return "Unknown"

def get_all_vendor_codes_for_client(client_tax_id: str) -> Dict[str, str]:
    c = _norm_tax_id(client_tax_id)
    return dict(VENDOR_CODE_BY_CLIENT.get(c, {}))

# ============================================================
# Category mapping for description/group
# ============================================================
def get_expense_category(description: str, platform: str = "") -> str:
    """
    Rules:
      - Lazada / Shopee / TikTok → Marketplace Expense
      - Commission → Selling Expense
      - Advertising → Advertising Expense
      - Goods → Inventory / COGS
      - Shipping/SPX → Shipping Expense
    """
    desc = _norm_name(description)
    plat = _norm_name(platform)

    if any(w in desc for w in ("shipping", "delivery", "ขนส่ง", "จัดส่ง", "spx")) or plat in ("spx", "spx express"):
        return "Shipping Expense"

    if any(w in desc for w in ("commission", "คอมมิชชั่น", "ค่าคอม")):
        return "Selling Expense"

    if any(w in desc for w in ("advertising", "โฆษณา", "ads", "sponsored")):
        return "Advertising Expense"

    if any(w in desc for w in ("goods", "สินค้า", "inventory", "cogs", "cost of goods")):
        return "Inventory / COGS"

    if plat in ("shopee", "lazada", "tiktok", "ช้อปปี้", "ลาซาด้า", "ติ๊กต๊อก"):
        return "Marketplace Expense"
    if any(w in desc for w in ("shopee", "lazada", "tiktok", "ช้อปปี้", "ลาซาด้า", "ติ๊กต๊อก")):
        return "Marketplace Expense"

    return "Marketplace Expense"

def format_short_description(platform: str, fee_type: str = "", seller_info: str = "") -> str:
    parts = []
    if platform:
        parts.append(platform.strip())
    if fee_type:
        parts.append(fee_type.strip())

    if seller_info:
        m = re.search(r"Seller(?:\s+ID)?:\s*([0-9A-Za-z_\-]+)", seller_info)
        if m:
            parts.append(f"Seller {m.group(1)}")

    return " - ".join(parts) if parts else "Marketplace Expense"

__all__ = [
    "get_vendor_code",
    "get_vendor_tax_id_from_name",
    "detect_client_from_context",
    "get_client_name",
    "get_all_vendor_codes_for_client",
    "get_expense_category",
    "format_short_description",
    "CLIENT_RABBIT",
    "CLIENT_SHD",
    "CLIENT_TOPONE",
    "VENDOR_SHOPEE",
    "VENDOR_LAZADA",
    "VENDOR_TIKTOK",
    "VENDOR_SPX",
    "VENDOR_MARKETPLACE_OTHER",
    "VENDOR_SHOPIFY",
]
