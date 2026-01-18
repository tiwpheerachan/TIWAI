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

# ✅ platform normalization (must match job_worker/platform_constants)
from .platform_constants import normalize_platform as _norm_platform

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
    _SPX_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_spx = None  # type: ignore
    _SPX_EXTRACTOR_OK = False

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

# ✅ AI extractor (optional)
try:
    from .ai_service import ai_fill_peak_row as extract_with_ai  # platform-aware JSON patch
    _AI_OK = True
except Exception:  # pragma: no cover
    try:
        from .ai_extract_service import extract_with_ai  # type: ignore
        _AI_OK = True
    except Exception:
        extract_with_ai = None  # type: ignore
        _AI_OK = False


logger = logging.getLogger(__name__)

# ============================================================
# Platform groups + defaults (กลาง)
# ============================================================

PLATFORM_GROUPS = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Marketplace Expense",  # คุณใช้ Marketplace Expense สำหรับโลจิสติกส์ด้วย
    "THAI_TAX": "General Expense",
    "UNKNOWN": "Other Expense",
    "GENERIC": "Other Expense",
}

PLATFORM_DESCRIPTIONS = {
    "META": "Meta Ads",
    "GOOGLE": "Google Ads",
    "SHOPEE": "Shopee Marketplace",
    "LAZADA": "Lazada Marketplace",
    "TIKTOK": "TikTok Shop",
    "SPX": "Shopee Express",
    "THAI_TAX": "Tax Invoice",
    "UNKNOWN": "",
    "GENERIC": "",
}

# ============================================================
# Client constants (ตามที่คุณใช้ใน UI)
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD    = "0105563022918"
CLIENT_TOPONE = "0105565027615"

DEFAULT_COMPANY_NAME_BY_TAX = {
    CLIENT_RABBIT: "RABBIT",
    CLIENT_SHD: "SHD",
    CLIENT_TOPONE: "TOPONE",
}

# ============================================================
# PEAK A–U schema lock
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

# keys ที่ “ห้าม AI ไปย้ายคอลัมน์/ทำเลื่อน”
_AI_BLACKLIST_KEYS = {"T_note", "U_group", "K_account"}
_INTERNAL_OK_PREFIXES = ("_",)

_RE_ALL_WS = re.compile(r"\s+")

# ============================================================
# Reference normalizer (ตัด Shopee-TIV- ให้เหลือ TRS...)
# ============================================================

RE_TRS_CORE = re.compile(r"(TRS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_RCS_CORE = re.compile(r"(RCS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_TTSTH_CORE = re.compile(r"(TTSTH\d{10,})", re.IGNORECASE)

RE_LEADING_NOISE_PREFIX = re.compile(
    r"^(?:Shopee-)?TI[VR]-|^Shopee-|^TIV-|^TIR-|^SPX-|^LAZ-|^LZD-|^TikTok-",
    re.IGNORECASE,
)

def _strip_ext(s: str) -> str:
    return re.sub(r"\.(pdf|png|jpg|jpeg|xlsx|xls)$", "", s, flags=re.IGNORECASE).strip()

# ============================================================
# helpers: sanitize / merge / compact
# ============================================================

def _sanitize_incoming_row(d: Any) -> Dict[str, Any]:
    return d if isinstance(d, dict) else {}

def _compact_no_ws(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return _RE_ALL_WS.sub("", s)

def _normalize_reference_core(value: Any) -> str:
    """
    Normalize reference/invoice ให้เป็นแกนเอกสารที่ถูกต้อง
    Example:
      "Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf" -> "TRSPEMKP00-00000-251203-0012589"
    """
    s = _compact_no_ws(value)
    if not s:
        return ""
    s = _strip_ext(s)

    # ดึง core ถ้ามี
    for pat in (RE_TRS_CORE, RE_RCS_CORE, RE_TTSTH_CORE):
        m = pat.search(s)
        if m:
            return _compact_no_ws(m.group(1))

    # ตัด prefix noise
    s2 = RE_LEADING_NOISE_PREFIX.sub("", s).strip()
    s2 = _strip_ext(s2)
    return _compact_no_ws(s2) if s2 else _compact_no_ws(s)

def _try_get_source_filename(filename: str, row: Dict[str, Any]) -> str:
    """
    ใช้ filename ที่ส่งเข้ามาเป็นหลัก ถ้าไม่มีค่อยดูใน meta ของ row
    """
    if filename:
        try:
            return os.path.basename(str(filename))
        except Exception:
            return str(filename)

    for k in ("_filename", "filename", "source_file", "_source_file", "_file", "file"):
        v = row.get(k)
        if v:
            try:
                return os.path.basename(str(v))
            except Exception:
                return str(v)
    return ""

def _sanitize_ai_row(ai: Dict[str, Any]) -> Dict[str, Any]:
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

def _merge_rows(base: Dict[str, Any], patch: Dict[str, Any], *, fill_missing: bool = True) -> Dict[str, Any]:
    if not patch:
        return base

    out = dict(base)
    for k, v in patch.items():
        if not k or k in _AI_BLACKLIST_KEYS:
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

    if row.get("J_price_type") and not validate_price_type(row.get("J_price_type", "")):
        errors.append("ประเภทราคาไม่ถูกต้อง")

    if row.get("O_vat_rate") and not validate_vat_rate(row.get("O_vat_rate", "")):
        errors.append("อัตราภาษีไม่ถูกต้อง")

    return errors

# ============================================================
# extractor call (backward compatible) + MUST PASS filename+cfg
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
    cfg = cfg or {}

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        kwargs: Dict[str, Any] = {}

        if "filename" in params:
            kwargs["filename"] = filename
        if "client_tax_id" in params and client_tax_id:
            kwargs["client_tax_id"] = client_tax_id
        if "cfg" in params:
            kwargs["cfg"] = cfg
        if "platform_hint" in params and platform_hint:
            kwargs["platform_hint"] = platform_hint

        if kwargs:
            return fn(text, **kwargs)  # type: ignore[arg-type]
    except Exception:
        pass

    if client_tax_id:
        try:
            return fn(text, client_tax_id=client_tax_id)  # type: ignore
        except TypeError:
            pass

    return fn(text)  # type: ignore

# ============================================================
# Vendor mapping: force D_vendor_code = Cxxxxx
# ============================================================

def _apply_vendor_code_mapping(row: Dict[str, Any], text: str, client_tax_id: str) -> Dict[str, Any]:
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
# Platform-specific enforcement (กลาง)
# ============================================================

def _enforce_platform_rules(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    p = (platform or "").upper().strip()

    # group default
    if p in PLATFORM_GROUPS and not str(row.get("U_group") or "").strip():
        row["U_group"] = PLATFORM_GROUPS[p]

    # description default (only if extractor didn't fill)
    if not str(row.get("L_description") or "").strip():
        desc = PLATFORM_DESCRIPTIONS.get(p, "")
        if desc:
            row["L_description"] = desc

    # VAT defaults
    if p in ("META", "GOOGLE"):
        if not str(row.get("O_vat_rate") or "").strip():
            row["O_vat_rate"] = "NO"
        if not str(row.get("J_price_type") or "").strip():
            row["J_price_type"] = "3"
    elif p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX"):
        if not str(row.get("O_vat_rate") or "").strip():
            row["O_vat_rate"] = "7%"
        if not str(row.get("J_price_type") or "").strip():
            row["J_price_type"] = "1"

    # Marketplace bucket
    if p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX"):
        row["U_group"] = "Marketplace Expense"
        if str(row.get("K_account") or "").strip() == "Marketplace Expense":
            row["K_account"] = ""

    return row

# ============================================================
# LOCK schema A–U (กันคอลัมน์เลื่อน)
# ============================================================

def lock_peak_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    safe = _sanitize_incoming_row(row)
    out: Dict[str, Any] = {}

    for k, v in safe.items():
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            out[k] = v

    for k in PEAK_KEYS_ORDER:
        out[k] = safe.get(k, "")

    return out

# ============================================================
# ✅ Finalize helpers: company, GL code, description structure
# ============================================================

def _resolve_client_tax_id(text: str, client_tax_id: str, cfg: Dict[str, Any]) -> str:
    ctax = (client_tax_id or "").strip()
    if ctax:
        return ctax
    # allow cfg override
    ctax = str(cfg.get("client_tax_id") or "").strip()
    if ctax:
        return ctax
    # detect from context (optional)
    if detect_client_from_context is not None:
        try:
            ctax = (detect_client_from_context(text) or "").strip()
        except Exception:
            ctax = ""
    return ctax

def _resolve_company_name(client_tax_id: str, cfg: Dict[str, Any]) -> str:
    # cfg override
    mp = cfg.get("company_name_by_tax_id")
    if isinstance(mp, dict):
        v = mp.get(client_tax_id)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # env override (optional)
    if client_tax_id == CLIENT_RABBIT and os.getenv("COMPANY_NAME_RABBIT"):
        return os.getenv("COMPANY_NAME_RABBIT", "").strip()
    if client_tax_id == CLIENT_SHD and os.getenv("COMPANY_NAME_SHD"):
        return os.getenv("COMPANY_NAME_SHD", "").strip()
    if client_tax_id == CLIENT_TOPONE and os.getenv("COMPANY_NAME_TOPONE"):
        return os.getenv("COMPANY_NAME_TOPONE", "").strip()

    return DEFAULT_COMPANY_NAME_BY_TAX.get(client_tax_id, "")

def _resolve_gl_code(client_tax_id: str, platform: str, row: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    """
    เติม K_account ให้ครบ:
    - แนะนำให้ใส่ cfg["gl_code_map"] เป็น dict
      รูปแบบรองรับ:
        1) {"0105...": "GLxxx"}  (ใช้ร่วมทุก platform)
        2) {"0105...": {"MARKETPLACE":"...", "ADS":"...", "DEFAULT":"..."}} (ละเอียด)
    - หรือ env: GL_CODE_RABBIT/SHD/TOPONE
    - ถ้าไม่มีจริงๆ จะ fallback เป็น U_group เพื่อไม่ให้ K_account ว่าง
    """
    # 1) cfg map
    mp = cfg.get("gl_code_map")
    if isinstance(mp, dict):
        v = mp.get(client_tax_id)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            p = (platform or "").upper()
            bucket = "ADS" if p in ("META", "GOOGLE") else "MARKETPLACE" if p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX") else "DEFAULT"
            vv = v.get(bucket) or v.get("DEFAULT") or ""
            if isinstance(vv, str) and vv.strip():
                return vv.strip()

    # 2) env
    if client_tax_id == CLIENT_RABBIT and os.getenv("GL_CODE_RABBIT"):
        return os.getenv("GL_CODE_RABBIT", "").strip()
    if client_tax_id == CLIENT_SHD and os.getenv("GL_CODE_SHD"):
        return os.getenv("GL_CODE_SHD", "").strip()
    if client_tax_id == CLIENT_TOPONE and os.getenv("GL_CODE_TOPONE"):
        return os.getenv("GL_CODE_TOPONE", "").strip()

    # 3) fallback: if extractor already filled
    cur = str(row.get("K_account") or "").strip()
    if cur:
        return cur

    # 4) last fallback: use group (กัน import พังเพราะว่าง)
    grp = str(row.get("U_group") or "").strip()
    return grp

def _guess_seller_id(row: Dict[str, Any], text: str) -> str:
    # common keys from scrapers/extractors
    for k in ("seller_id", "sellerId", "shop_id", "shopid", "shopId", "merchant_id", "merchantId"):
        v = row.get(k)
        if v:
            s = str(v).strip()
            if s:
                return s
    # attempt from text
    m = re.search(r"(?:seller\s*id|shop\s*id)\s*[:#]?\s*([0-9]{4,})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""

def _guess_username(row: Dict[str, Any], text: str) -> str:
    for k in ("username", "user_name", "seller_username", "shop_name", "shopName", "sellerName"):
        v = row.get(k)
        if v:
            s = str(v).strip()
            if s:
                return s
    m = re.search(r"(?:username|user\s*name|shop\s*name)\s*[:#]?\s*([A-Za-z0-9_.\-]{3,})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""

def _build_description_structure(
    base_desc: str,
    platform: str,
    seller_id: str,
    username: str,
    src_file: str,
) -> str:
    """
    Desc structure: คง base_desc เดิม แล้ว append tags ที่คุณต้องการ
    """
    parts: List[str] = []
    bd = (base_desc or "").strip()
    if not bd:
        bd = PLATFORM_DESCRIPTIONS.get((platform or "").upper(), "") or ""
    if bd:
        parts.append(bd)

    tags: List[str] = []
    if seller_id:
        tags.append(f"SellerID={seller_id}")
    if username:
        tags.append(f"Username={username}")
    if src_file:
        tags.append(f"File={src_file}")

    if tags:
        parts.append(" | ".join(tags))

    return " — ".join([p for p in parts if p.strip()]).strip()

# ============================================================
# ✅ FINALIZE (THE IMPORTANT PART)
# ============================================================

def finalize_row(
    row: Dict[str, Any],
    *,
    platform: str,
    text: str,
    filename: str,
    client_tax_id: str,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    ทำให้ stable + import-safe:
    - A_company_name เติมตาม client_tax_id
    - O_vat_rate เติมตาม platform (Ads=NO / Marketplace=7%)
    - P_wht: ไม่ล้างทิ้ง (ถ้าไม่มีให้เป็น "")
    - C_reference/G_invoice_no: normalize ให้เหลือ TRS... (ตัด Shopee-TIV-) + sync กัน
    - L_description: ตาม Desc structure + SellerID/Username/File
    - K_account: เติม GL code ตามบริษัท (cfg/env) ถ้าไม่มี fallback เป็น group
    - T_note: policy ต้องว่าง
    - lock schema A–U กันคอลัมน์เลื่อน
    """
    row = _sanitize_incoming_row(row)
    p = (platform or "UNKNOWN").upper().strip()
    cfg = cfg or {}

    # policy: T_note must be empty
    row["T_note"] = ""

    # resolve client tax id + company
    ctax = _resolve_client_tax_id(text, client_tax_id, cfg)
    if ctax and not str(row.get("A_company_name") or "").strip():
        row["A_company_name"] = _resolve_company_name(ctax, cfg)

    # enforce platform rules (group/desc/vat defaults)
    row = _enforce_platform_rules(row, p)

    # ✅ keep P_wht (don't wipe). Ensure exists
    if row.get("P_wht") is None:
        row["P_wht"] = ""
    else:
        row["P_wht"] = str(row.get("P_wht") or "").strip()

    # ✅ normalize references (prefer filename core)
    src_file = _try_get_source_filename(filename, row)
    ref_from_file = _normalize_reference_core(src_file) if src_file else ""
    ref_c = _normalize_reference_core(row.get("C_reference", ""))
    ref_g = _normalize_reference_core(row.get("G_invoice_no", ""))
    best_ref = ref_from_file or ref_c or ref_g

    row["C_reference"] = best_ref
    row["G_invoice_no"] = best_ref

    # also compact no-ws again (safety)
    row["C_reference"] = _compact_no_ws(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_no_ws(row.get("G_invoice_no", ""))

    # ✅ description structure + seller id/username/file
    seller_id = _guess_seller_id(row, text)
    username = _guess_username(row, text)

    base_desc = str(row.get("L_description") or "").strip()
    row["L_description"] = _build_description_structure(
        base_desc=base_desc,
        platform=p,
        seller_id=seller_id,
        username=username,
        src_file=src_file,
    )

    # ✅ GL code fill
    if not str(row.get("K_account") or "").strip():
        row["K_account"] = _resolve_gl_code(ctax, p, row, cfg)

    # minimal defaults (กัน PEAK import error)
    row.setdefault("A_seq", "")
    row.setdefault("J_price_type", row.get("J_price_type") or ("3" if p in ("META", "GOOGLE") else "1"))
    row.setdefault("M_qty", row.get("M_qty") or "1")
    if not str(row.get("O_vat_rate") or "").strip():
        row["O_vat_rate"] = "NO" if p in ("META", "GOOGLE") else "7%"

    # lock schema
    row = lock_peak_columns(row)
    return row

def _record_ai_error(row: Dict[str, Any], stage: str, exc: Exception) -> None:
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
# Platform normalization mapping: classifier -> router
# ============================================================

def _normalize_platform_label(platform_raw: str) -> str:
    p = _norm_platform(platform_raw) or "UNKNOWN"
    if p in ("META", "GOOGLE", "SHOPEE", "LAZADA", "TIKTOK", "SPX", "THAI_TAX"):
        return p
    if p in ("UNKNOWN", ""):
        return "GENERIC"
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
    ✅ MUST PASS filename + cfg ลงไปถึง extractor ทุกตัว

    Returns:
        (platform_for_job_worker, row, errors)
    """
    text = text or ""
    filename = filename or ""
    client_tax_id = (client_tax_id or "").strip()
    cfg = cfg or {}

    # 1) classify
    try:
        try:
            sig = inspect.signature(classify_platform)
            params = sig.parameters
            if "cfg" in params:
                platform_raw = classify_platform(text, filename=filename, cfg=cfg)
            else:
                platform_raw = classify_platform(text, filename=filename)
        except Exception:
            platform_raw = classify_platform(text, filename=filename)
    except Exception as e:
        logger.exception("classify_platform failed: %s", e)
        platform_raw = "UNKNOWN"

    platform_route = _normalize_platform_label(platform_raw)
    platform_out = platform_route if platform_route != "GENERIC" else "UNKNOWN"

    logger.info("Platform classified: %s -> route=%s (file=%s)", platform_raw, platform_route, filename)

    # 2) route to extractor
    try:
        if platform_route == "META":
            if _META_EXTRACTOR_OK and extract_meta_ads is not None:
                row = _safe_call_extractor(
                    extract_meta_ads, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="META",
                )
                row["_extraction_method"] = "rule_based_meta"
            else:
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai, text,
                        filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="META",
                    )
                    row["_extraction_method"] = "ai_meta_fallback"
                else:
                    row = _safe_call_extractor(
                        extract_generic, text,
                        filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="META",
                    )
                    row["_extraction_method"] = "generic_meta_fallback"
                    row["_missing_extractor"] = "meta"

        elif platform_route == "GOOGLE":
            if _GOOGLE_EXTRACTOR_OK and extract_google_ads is not None:
                row = _safe_call_extractor(
                    extract_google_ads, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="GOOGLE",
                )
                row["_extraction_method"] = "rule_based_google"
            else:
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai, text,
                        filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="GOOGLE",
                    )
                    row["_extraction_method"] = "ai_google_fallback"
                else:
                    row = _safe_call_extractor(
                        extract_generic, text,
                        filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="GOOGLE",
                    )
                    row["_extraction_method"] = "generic_google_fallback"
                    row["_missing_extractor"] = "google"

        elif platform_route == "SHOPEE":
            row = _safe_call_extractor(
                extract_shopee, text,
                filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="SHOPEE",
            )
            row["_extraction_method"] = "rule_based_shopee"

        elif platform_route == "LAZADA":
            row = _safe_call_extractor(
                extract_lazada, text,
                filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="LAZADA",
            )
            row["_extraction_method"] = "rule_based_lazada"

        elif platform_route == "TIKTOK":
            row = _safe_call_extractor(
                extract_tiktok, text,
                filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="TIKTOK",
            )
            row["_extraction_method"] = "rule_based_tiktok"

        elif platform_route == "SPX":
            if _SPX_EXTRACTOR_OK and extract_spx is not None:
                row = _safe_call_extractor(
                    extract_spx, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="SPX",
                )
                row["_extraction_method"] = "rule_based_spx"
            else:
                row = _safe_call_extractor(
                    extract_generic, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="SPX",
                )
                row["_extraction_method"] = "generic_spx_fallback"
                row["_missing_extractor"] = "spx"

        elif platform_route == "THAI_TAX":
            if _AI_OK and extract_with_ai is not None:
                row = _safe_call_extractor(
                    extract_with_ai, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="THAI_TAX",
                )
                row["_extraction_method"] = "ai_thai_tax"
            else:
                row = _safe_call_extractor(
                    extract_generic, text,
                    filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="THAI_TAX",
                )
                row["_extraction_method"] = "generic_thai_tax_fallback"

        else:
            row = _safe_call_extractor(
                extract_generic, text,
                filename=filename, client_tax_id=client_tax_id, cfg=cfg, platform_hint="GENERIC",
            )
            row["_extraction_method"] = "generic"

    except Exception as e:
        logger.exception("Extractor error (platform=%s, file=%s)", platform_route, filename)
        row = _sanitize_incoming_row(extract_generic(text))
        row["_extractor_error"] = f"{type(e).__name__}: {str(e)}"[:500]
        row["_extraction_method"] = "generic_error_fallback"

    row = _sanitize_incoming_row(row)

    # 2.1 minimal defaults (อย่าบังคับค่าแบบทำลายของ extractor)
    row.setdefault("A_seq", "")
    if row.get("M_qty") in ("", None):
        row["M_qty"] = "1"

    # debug meta
    if os.getenv("STORE_CLASSIFIER_META", "1") == "1":
        row["_platform"] = platform_out
        row["_platform_route"] = platform_route
        row["_platform_raw"] = platform_raw
        row["_filename"] = filename
        if cfg:
            row["_cfg"] = str(cfg)[:300]

    # 3) optional AI enhancement for non-meta/google
    should_enhance = (
        platform_route not in ("META", "GOOGLE")
        and _AI_OK
        and extract_with_ai is not None
        and os.getenv("ENABLE_AI_EXTRACT", "0") == "1"
    )

    if should_enhance:
        try:
            ai_raw = _safe_call_extractor(
                extract_with_ai,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint=platform_out,
            )
            ai_row = _sanitize_ai_row(_sanitize_incoming_row(ai_raw))
            fill_missing = os.getenv("AI_FILL_MISSING", "1") == "1"
            row = _merge_rows(row, ai_row, fill_missing=fill_missing)
            if row.get("_extraction_method"):
                row["_extraction_method"] = f"{row['_extraction_method']}+ai"
        except Exception as e:
            logger.warning("AI enhancement failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_enhance", e)

    # 4) validate (ก่อน finalize เพื่อดู error เดิม)
    errors = _validate_row(row)

    # 5) optional AI repair pass if errors
    if (
        errors
        and platform_route not in ("META", "GOOGLE")
        and _AI_OK
        and extract_with_ai is not None
        and os.getenv("AI_REPAIR_PASS", "0") == "1"
    ):
        try:
            prompt = (text or "") + "\n\n# VALIDATION_ERRORS\n" + "\n".join(errors)
            ai_fix_raw = _safe_call_extractor(
                extract_with_ai,
                prompt,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint=platform_out,
            )
            ai_fix = _sanitize_ai_row(_sanitize_incoming_row(ai_fix_raw))
            row = _merge_rows(row, ai_fix, fill_missing=False)
            errors = _validate_row(row)
        except Exception as e:
            logger.warning("AI repair failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_repair", e)

    # 6) vendor mapping pass (force Cxxxxx)
    row = _apply_vendor_code_mapping(row, text, client_tax_id)

    # 7) ✅ FINALIZE + LOCK (นี่คือสิ่งที่ทำให้ import ไม่พัง)
    row = finalize_row(
        row,
        platform=platform_out,
        text=text,
        filename=filename,
        client_tax_id=client_tax_id,
        cfg=cfg,
    )

    return platform_out, row, errors


__all__ = [
    "extract_row_from_text",
    "finalize_row",
    "PEAK_KEYS_ORDER",
    "PLATFORM_GROUPS",
    "PLATFORM_DESCRIPTIONS",
]
