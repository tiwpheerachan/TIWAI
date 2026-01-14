# backend/app/services/document_router.py
"""
Document Router - Enhanced Version for 8 Platforms

✅ Improvements:
1. ✅ Support for 8 platforms (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
2. ✅ Meta/Google Ads routing (rule-based extractors)
3. ✅ Thai Tax Invoice routing
4. ✅ Integration with enhanced classifier (8 platforms)
5. ✅ Integration with enhanced AI service (platform-aware)
6. ✅ Integration with export_service constants
7. ✅ Better error handling and logging
8. ✅ cfg parameter support for job_worker
9. ✅ Backward compatibility maintained
10. ✅ Platform-specific validation
"""
from __future__ import annotations

from typing import Dict, Any, Tuple, List, Callable, Optional
import os
import logging
import inspect
import re

from .classifier import classify_platform

from ..extractors.generic import extract_generic
from ..extractors.shopee import extract_shopee
from ..extractors.lazada import extract_lazada
from ..extractors.tiktok import extract_tiktok

# ✅ Meta/Google Ads extractors (rule-based)
try:
    from ..extractors.ads_meta import extract_meta_ads
    _META_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_meta_ads = None  # type: ignore
    _META_EXTRACTOR_OK = False

try:
    from ..extractors.ads_google import extract_google_ads
    _GOOGLE_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_google_ads = None  # type: ignore
    _GOOGLE_EXTRACTOR_OK = False

# ✅ SPX extractor (optional)
try:
    from ..extractors.spx import extract_spx  # type: ignore
except Exception:  # pragma: no cover
    extract_spx = None  # type: ignore

# ✅ Vendor code mapping (Cxxxxx)
try:
    from ..extractors.vendor_mapping import get_vendor_code, detect_client_from_context
    _VENDOR_MAPPING_OK = True
except Exception:  # pragma: no cover
    get_vendor_code = None  # type: ignore
    detect_client_from_context = None  # type: ignore
    _VENDOR_MAPPING_OK = False

from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

# ✅ AI extractor (optional, enhanced version)
try:
    from .ai_service import ai_fill_peak_row as extract_with_ai
    _AI_OK = True
except Exception:  # pragma: no cover
    try:
        from .ai_extract_service import extract_with_ai
        _AI_OK = True
    except Exception:
        extract_with_ai = None  # type: ignore
        _AI_OK = False

logger = logging.getLogger(__name__)

# ============================================================
# Platform Constants (aligned with export_service & ai_service)
# ============================================================

# ✅ Platform groups (from export_service)
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

# ✅ Platform-specific default descriptions
PLATFORM_DESCRIPTIONS = {
    "META": "Meta Ads",
    "GOOGLE": "Google Ads",
    "SHOPEE": "Shopee Marketplace",
    "LAZADA": "Lazada Marketplace",
    "TIKTOK": "TikTok Shop",
    "SPX": "Shopee Express Delivery",
    "THAI_TAX": "",  # Variable
    "UNKNOWN": "",
}

# ============================================================
# PEAK columns lock (A-U) — อย่าให้คอลัมน์เลื่อนอีก
# ============================================================
PEAK_KEYS_ORDER: List[str] = [
    "A_seq",
    "A_company_name",
    "B_doc_date",
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "H_invoice_date",
    "I_tax_purchase_date",
    "J_price_type",
    "K_account",
    "L_description",
    "M_qty",
    "N_unit_price",
    "O_vat_rate",
    "P_wht",
    "Q_payment_method",
    "R_paid_amount",
    "S_pnd",
    "T_note",
    "U_group",
]

# กัน AI / extractor เขียนทับ key ที่ทำให้ "เลื่อนช่อง"
_AI_BLACKLIST_KEYS = {"T_note", "U_group", "K_account"}

# คีย์ภายในที่อนุญาตให้ผ่านได้ (metadata)
_INTERNAL_OK_PREFIXES = ("_",)

# ✅ whitespace compact for ref/invoice
_RE_ALL_WS = re.compile(r"\s+")

# ============================================================
# helpers: safe merge + sanitize
# ============================================================

def _sanitize_incoming_row(d: Any) -> Dict[str, Any]:
    """รับเฉพาะ dict เท่านั้น"""
    return d if isinstance(d, dict) else {}


def _compact_no_ws(v: Any) -> str:
    """
    ✅ ตัด whitespace ทั้งหมด (space/newline/tab) ให้ token ติดกัน
    เช่น "RCSPX...-25 1218-0001" -> "RCSPX...-251218-0001"
    """
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return _RE_ALL_WS.sub("", s)


def _sanitize_ai_row(ai: Dict[str, Any]) -> Dict[str, Any]:
    """
    - ตัด key ต้องห้าม: T_note, U_group, K_account
    - ตัด None/"" ออก
    - รับเฉพาะ key ที่อยู่ใน PEAK_KEYS_ORDER หรือเป็น _meta
    """
    if not ai:
        return {}

    cleaned: Dict[str, Any] = {}
    for k, v in ai.items():
        if not k:
            continue
        if k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        if k in PEAK_KEYS_ORDER:
            cleaned[k] = v
            continue

        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            cleaned[k] = v
            continue

    return cleaned


def _merge_rows(
    base: Dict[str, Any],
    patch: Dict[str, Any],
    *,
    fill_missing: bool = True,
) -> Dict[str, Any]:
    """
    merge patch into base:
    - fill_missing=True  -> เติมเฉพาะช่องว่าง / 0 / 0.00
    - fill_missing=False -> ทับเฉพาะ key ที่ patch ส่งมา (แต่ไม่ทับ blacklist)
    """
    if not patch:
        return base

    out = dict(base)

    for k, v in patch.items():
        if not k:
            continue
        if k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        if fill_missing:
            cur = out.get(k, "")
            if cur in ("", None, "0", "0.00"):
                out[k] = v
        else:
            out[k] = v

    return out


# ============================================================
# helpers: validation
# ============================================================

def _validate_row(row: Dict[str, Any]) -> List[str]:
    """Validate row fields"""
    errors: List[str] = []

    if not validate_yyyymmdd(row.get("B_doc_date", "")):
        errors.append("วันที่เอกสารรูปแบบไม่ถูกต้อง")

    if row.get("H_invoice_date") and not validate_yyyymmdd(row.get("H_invoice_date", "")):
        errors.append("วันที่ใบกำกับฯรูปแบบไม่ถูกต้อง")

    if row.get("I_tax_purchase_date") and not validate_yyyymmdd(row.get("I_tax_purchase_date", "")):
        errors.append("วันที่ภาษีซื้อรูปแบบไม่ถูกต้อง")

    if row.get("F_branch_5") and not validate_branch5(row.get("F_branch_5", "")):
        errors.append("เลขสาขาไม่ใช่ 5 หลัก")

    if row.get("E_tax_id_13") and not validate_tax13(row.get("E_tax_id_13", "")):
        errors.append("เลขภาษีไม่ใช่ 13 หลัก")

    if not validate_price_type(row.get("J_price_type", "")):
        errors.append("ประเภทราคาไม่ถูกต้อง")

    if not validate_vat_rate(row.get("O_vat_rate", "")):
        errors.append("อัตราภาษีไม่ถูกต้อง")

    return errors


# ============================================================
# helpers: extractor call (backward compatible)
# ============================================================

def _safe_call_extractor(
    fn: Callable[..., Dict[str, Any]],
    text: str,
    *,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    platform_hint: str = "",
) -> Dict[str, Any]:
    """
    ✅ Enhanced: Call extractor with backward compatibility + cfg + platform_hint support
    
    - ถ้ารองรับ filename/client_tax_id/cfg/platform_hint -> ส่งให้
    - ถ้าไม่รองรับ -> fallback fn(text)
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters

        kwargs: Dict[str, Any] = {}
        if "filename" in params:
            kwargs["filename"] = filename
        if "client_tax_id" in params and client_tax_id:
            kwargs["client_tax_id"] = client_tax_id
        if "cfg" in params and cfg:
            kwargs["cfg"] = cfg
        if "platform_hint" in params and platform_hint:
            kwargs["platform_hint"] = platform_hint

        if kwargs:
            return fn(text, **kwargs)  # type: ignore[arg-type]
    except Exception:
        pass

    # Fallback attempts (backward compatibility)
    if client_tax_id:
        try:
            return fn(text, client_tax_id=client_tax_id)  # type: ignore
        except TypeError:
            pass

    return fn(text)  # type: ignore


# ============================================================
# ✅ Vendor code mapping pass (force D_vendor_code = Cxxxxx)
# ============================================================

def _apply_vendor_code_mapping(row: Dict[str, Any], text: str, client_tax_id: str) -> Dict[str, Any]:
    """
    Force D_vendor_code to be Cxxxxx using vendor_mapping.py
    """
    if not isinstance(row, dict):
        return row

    if not _VENDOR_MAPPING_OK or get_vendor_code is None:
        return row

    ctax = (client_tax_id or "").strip()
    if not ctax and detect_client_from_context is not None:
        try:
            ctax = detect_client_from_context(text) or ""
        except Exception:
            ctax = ""

    if not ctax:
        return row

    vtax = str(row.get("E_tax_id_13") or "").strip()
    vname = str(row.get("D_vendor_code") or "").strip()

    try:
        code = get_vendor_code(client_tax_id=ctax, vendor_tax_id=vtax, vendor_name=vname)
    except Exception:
        return row

    if isinstance(code, str) and code.startswith("C") and len(code) >= 5:
        row["D_vendor_code"] = code
        if os.getenv("STORE_VENDOR_MAPPING_META", "1") == "1":
            row["_client_tax_id_used"] = ctax
            row["_vendor_tax_id_used"] = vtax or ""
            row["_vendor_code_resolved"] = code

    return row


# ============================================================
# ✅ Platform-specific enforcement
# ============================================================

def _enforce_platform_rules(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """
    ✅ Enforce platform-specific rules (from export_service logic)
    
    - META/GOOGLE: Advertising Expense, specific VAT rules
    - Marketplace: Marketplace Expense
    - SPX: Delivery/Logistics Expense
    - THAI_TAX: General Expense
    """
    p = (platform or "").upper().strip()
    
    # Set U_group based on platform
    if p in PLATFORM_GROUPS:
        if not row.get("U_group") or row.get("U_group") == "":
            row["U_group"] = PLATFORM_GROUPS[p]
    
    # Set L_description if empty
    if not row.get("L_description") and p in PLATFORM_DESCRIPTIONS:
        desc = PLATFORM_DESCRIPTIONS[p]
        if desc:
            row["L_description"] = desc
    
    # Marketplace platforms: ensure correct group
    if p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX"):
        row["U_group"] = "Marketplace Expense"
        # Don't let it go into K_account
        if str(row.get("K_account", "") or "").strip() == "Marketplace Expense":
            row["K_account"] = ""
    
    return row


# ============================================================
# 🔥 FINAL LOCK: force PEAK schema
# ============================================================

def lock_peak_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    1) ล็อกให้มีแต่คอลัมน์ PEAK + _meta
    2) ใส่คีย์ที่ขาดให้ครบด้วยค่า "" (กัน CSV/Excel เลื่อน)
    3) ห้ามเอา key แปลก ๆ มาปน
    """
    safe = _sanitize_incoming_row(row)
    out: Dict[str, Any] = {}

    # Keep internal meta first
    for k, v in safe.items():
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            out[k] = v

    # Lock PEAK keys
    for k in PEAK_KEYS_ORDER:
        out[k] = safe.get(k, "")

    return out


def _finalize_row(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """
    Final sanitize:
    - P_wht ว่าง (คุณไม่ต้องการบันทึก)
    - T_note ว่าง
    - sync C/G + compact no whitespace
    - enforce platform rules
    - lock schema
    """
    # ✅ WHT must be empty (your export requirement)
    row["P_wht"] = ""

    # ✅ notes must be empty
    if os.getenv("FORCE_EMPTY_NOTE", "1") == "1":
        row["T_note"] = ""

    # ✅ sync references
    if not row.get("C_reference") and row.get("G_invoice_no"):
        row["C_reference"] = row.get("G_invoice_no", "")
    if not row.get("G_invoice_no") and row.get("C_reference"):
        row["G_invoice_no"] = row.get("C_reference", "")

    # ✅ compact reference / invoice
    row["C_reference"] = _compact_no_ws(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_no_ws(row.get("G_invoice_no", ""))

    # ✅ enforce platform rules
    row = _enforce_platform_rules(row, platform)

    # ✅ lock columns last (กันเลื่อน)
    row = lock_peak_columns(row)

    return row


def _record_ai_error(row: Dict[str, Any], stage: str, exc: Exception) -> None:
    """Record AI errors in metadata"""
    if os.getenv("STORE_AI_ERROR_META", "1") != "1":
        return
    msg = f"{stage}: {type(exc).__name__}: {str(exc)}"
    msg = msg[:500]
    arr = row.get("_ai_errors")
    if not isinstance(arr, list):
        arr = []
    arr.append(msg)
    row["_ai_errors"] = arr


# ============================================================
# ✅ Platform normalization (classifier → router mapping)
# ============================================================

def _normalize_platform_label(platform: str) -> str:
    """
    ✅ Normalize classifier output to router platform
    
    Classifier returns: META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN
    Router needs: META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, GENERIC
    
    Note: UNKNOWN → GENERIC (for extractor routing)
    """
    p = (platform or "").upper().strip()
    
    # Known platforms (exact match)
    if p in ("META", "GOOGLE", "SHOPEE", "LAZADA", "TIKTOK", "SPX", "THAI_TAX"):
        return p
    
    # Legacy lowercase support (backward compatibility)
    p_lower = p.lower()
    legacy_map = {
        "shopee": "SHOPEE",
        "lazada": "LAZADA",
        "tiktok": "TIKTOK",
        "spx": "SPX",
        "ads": "GENERIC",  # Generic ads (not Meta/Google specific)
        "other": "GENERIC",
        "unknown": "GENERIC",
    }
    
    if p_lower in legacy_map:
        return legacy_map[p_lower]
    
    # Fallback
    return "GENERIC"


# ============================================================
# 🔥 MAIN ENTRY
# ============================================================

def extract_row_from_text(
    text: str,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any], List[str]]:
    """
    ✅ Enhanced: Extract row with 8 platforms support
    
    Args:
        text: Document text (OCR)
        filename: Filename for hints
        client_tax_id: Client tax ID (if known)
        cfg: Job config (optional, contains client_tags/platforms/strictMode)
    
    Returns:
        (platform, row, errors)
        
    Platform routing:
    - META → extract_meta_ads (rule-based, fast, accurate)
    - GOOGLE → extract_google_ads (rule-based, fast, accurate)
    - SHOPEE → extract_shopee
    - LAZADA → extract_lazada
    - TIKTOK → extract_tiktok
    - SPX → extract_spx (if available)
    - THAI_TAX → AI extraction (platform-aware)
    - GENERIC → extract_generic + AI enhancement
    """

    # 0) normalize inputs (safe)
    text = text or ""
    filename = filename or ""
    client_tax_id = (client_tax_id or "").strip()
    cfg = cfg or {}

    # 1) ✅ Classify with enhanced classifier (8 platforms)
    try:
        # Try to pass cfg to classifier if it supports it
        try:
            sig = inspect.signature(classify_platform)
            if "cfg" in sig.parameters:
                platform_raw = classify_platform(text, filename=filename, cfg=cfg)
            else:
                platform_raw = classify_platform(text, filename=filename)
        except Exception:
            platform_raw = classify_platform(text, filename=filename)
    except Exception as e:
        logger.exception("classify_platform failed: %s", e)
        platform_raw = "UNKNOWN"

    # 1.1) ✅ Normalize platform label
    platform = _normalize_platform_label(platform_raw)
    
    logger.info(f"Platform classified: {platform_raw} → {platform} (file: {filename})")

    # 2) ✅ Route to appropriate extractor
    try:
        # ========== Priority 1: Meta Ads (Rule-based) ==========
        if platform == "META":
            if _META_EXTRACTOR_OK and extract_meta_ads is not None:
                logger.info(f"Using Meta Ads extractor (rule-based)")
                row = _safe_call_extractor(
                    extract_meta_ads,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="META"
                )
                row["_extraction_method"] = "rule_based_meta"
            else:
                logger.warning("Meta extractor not available, using AI")
                # Fallback to AI with META hint
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai,
                        text,
                        filename=filename,
                        platform_hint="META"
                    )
                    row["_extraction_method"] = "ai_meta_fallback"
                else:
                    row = _safe_call_extractor(extract_generic, text, filename=filename)
                    row["_extraction_method"] = "generic_meta_fallback"
                    row["_missing_extractor"] = "meta"

        # ========== Priority 2: Google Ads (Rule-based) ==========
        elif platform == "GOOGLE":
            if _GOOGLE_EXTRACTOR_OK and extract_google_ads is not None:
                logger.info(f"Using Google Ads extractor (rule-based)")
                row = _safe_call_extractor(
                    extract_google_ads,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="GOOGLE"
                )
                row["_extraction_method"] = "rule_based_google"
            else:
                logger.warning("Google extractor not available, using AI")
                # Fallback to AI with GOOGLE hint
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai,
                        text,
                        filename=filename,
                        platform_hint="GOOGLE"
                    )
                    row["_extraction_method"] = "ai_google_fallback"
                else:
                    row = _safe_call_extractor(extract_generic, text, filename=filename)
                    row["_extraction_method"] = "generic_google_fallback"
                    row["_missing_extractor"] = "google"

        # ========== Priority 3: Marketplace (Shopee, Lazada, TikTok) ==========
        elif platform == "SHOPEE":
            row = _safe_call_extractor(
                extract_shopee,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg
            )
            row["_extraction_method"] = "rule_based_shopee"

        elif platform == "LAZADA":
            row = _safe_call_extractor(
                extract_lazada,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg
            )
            row["_extraction_method"] = "rule_based_lazada"

        elif platform == "TIKTOK":
            row = _safe_call_extractor(
                extract_tiktok,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg
            )
            row["_extraction_method"] = "rule_based_tiktok"

        # ========== Priority 4: SPX (Logistics) ==========
        elif platform == "SPX":
            if extract_spx is not None:
                row = _safe_call_extractor(
                    extract_spx,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg
                )
                row["_extraction_method"] = "rule_based_spx"
            else:
                # Fallback to generic but keep meta
                logger.warning("SPX extractor not available, using generic")
                row = _safe_call_extractor(
                    extract_generic,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg
                )
                row["_extraction_method"] = "generic_spx_fallback"
                row["_missing_extractor"] = "spx"

        # ========== Priority 5: Thai Tax Invoice (AI-based) ==========
        elif platform == "THAI_TAX":
            logger.info("Thai Tax Invoice detected, using AI with platform hint")
            if _AI_OK and extract_with_ai is not None:
                row = _safe_call_extractor(
                    extract_with_ai,
                    text,
                    filename=filename,
                    platform_hint="THAI_TAX"
                )
                row["_extraction_method"] = "ai_thai_tax"
            else:
                # Fallback to generic
                row = _safe_call_extractor(extract_generic, text, filename=filename)
                row["_extraction_method"] = "generic_thai_tax_fallback"

        # ========== Fallback: Generic/Unknown ==========
        else:
            logger.info(f"Unknown/Generic platform, using generic extractor")
            row = _safe_call_extractor(
                extract_generic,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg
            )
            row["_extraction_method"] = "generic"

    except Exception as e:
        logger.exception("Extractor error (platform=%s, file=%s)", platform, filename)
        row = _sanitize_incoming_row(extract_generic(text))
        row["_extractor_error"] = f"{type(e).__name__}: {str(e)}"[:500]
        row["_extraction_method"] = "generic_error_fallback"

    row = _sanitize_incoming_row(row)

    # 2.1) ✅ Ensure minimal stable defaults BEFORE AI/validate
    row.setdefault("A_seq", "")
    row.setdefault("A_company_name", "")
    row.setdefault("J_price_type", row.get("J_price_type") or "1")
    row.setdefault("M_qty", row.get("M_qty") or "1")
    row.setdefault("O_vat_rate", row.get("O_vat_rate") or "7%")
    
    # Platform-specific defaults
    if platform in PLATFORM_DESCRIPTIONS:
        desc = PLATFORM_DESCRIPTIONS[platform]
        if desc and not row.get("L_description"):
            row.setdefault("L_description", desc)
    
    if platform in PLATFORM_GROUPS:
        if not row.get("U_group"):
            row.setdefault("U_group", PLATFORM_GROUPS[platform])

    # Store meta for debug (optional)
    if os.getenv("STORE_CLASSIFIER_META", "1") == "1":
        row["_platform"] = platform
        row["_platform_raw"] = platform_raw
        row["_filename"] = filename
        if cfg:
            row["_cfg"] = str(cfg)[:200]

    # 3) ✅ AI ENHANCEMENT (optional, only for non-rule-based)
    # Don't enhance Meta/Google as they're already rule-based and accurate
    should_enhance = (
        platform not in ("META", "GOOGLE") and
        _AI_OK and
        extract_with_ai is not None and
        os.getenv("ENABLE_AI_EXTRACT", "0") == "1"
    )
    
    if should_enhance:
        try:
            logger.info(f"AI enhancement for platform: {platform}")
            # Call AI with platform hint
            try:
                ai_raw = _safe_call_extractor(
                    extract_with_ai,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    platform_hint=platform
                )
            except TypeError:
                ai_raw = extract_with_ai(text, filename=filename)

            ai_row = _sanitize_ai_row(_sanitize_incoming_row(ai_raw))
            fill_missing = os.getenv("AI_FILL_MISSING", "1") == "1"
            row = _merge_rows(row, ai_row, fill_missing=fill_missing)
            
            # Update extraction method
            if row.get("_extraction_method"):
                row["_extraction_method"] = f"{row['_extraction_method']}+ai"
                
        except Exception as e:
            logger.warning("AI extract failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_extract", e)

    # 4) ✅ Validate
    errors = _validate_row(row)

    # 5) ✅ AI REPAIR PASS (optional, only if errors exist)
    if (
        errors and
        platform not in ("META", "GOOGLE") and  # Don't repair rule-based
        _AI_OK and
        extract_with_ai is not None and
        os.getenv("AI_REPAIR_PASS", "0") == "1"
    ):
        try:
            logger.info(f"AI repair pass for {len(errors)} errors")
            prompt = (text or "") + "\n\n# VALIDATION_ERRORS\n" + "\n".join(errors)
            try:
                ai_fix_raw = extract_with_ai(prompt, filename=filename, platform_hint=platform)
            except TypeError:
                ai_fix_raw = extract_with_ai(prompt, filename=filename)

            ai_fix = _sanitize_ai_row(_sanitize_incoming_row(ai_fix_raw))
            row = _merge_rows(row, ai_fix, fill_missing=False)
            errors = _validate_row(row)
        except Exception as e:
            logger.warning("AI repair failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_repair", e)

    # 6) ✅ Vendor code mapping pass (ก่อน finalize/lock)
    row = _apply_vendor_code_mapping(row, text, client_tax_id)

    # 7) ✅ FINALIZE (กันเลื่อน + compact + enforce rules)
    row = _finalize_row(row, platform)

    return platform, row, errors


__all__ = [
    "extract_row_from_text",
    "PEAK_KEYS_ORDER",
    "PLATFORM_GROUPS",
    "PLATFORM_DESCRIPTIONS",
]