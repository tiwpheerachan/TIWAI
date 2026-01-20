# backend/app/services/job_worker.py
"""
Job Worker - Locked Version (Doc+Ref Normalization + Description Pattern + Accounting Guards)

✅ Enhancements (ตามที่คุณขอ “ล็อก” ให้ถูก)
1) ✅ A_seq เป็น running number จริงทั้งงาน (ไม่ reset ต่อไฟล์/ต่อ batch)
2) ✅ Backend filtering จาก cfg ที่ frontend ส่งมา:
   - client_tags / client_tax_ids / platforms / strictMode
   - ถ้าไม่ match → _status = NEEDS_REVIEW + ใส่เหตุผลใน T_note + _status_reason
3) ✅ LOCK: C_reference == G_invoice_no เสมอ
   - ใช้ “ชื่อไฟล์เต็มๆ (stem)” เช่น TRSPEMKP00-00000-251203-0012589
   - normalize แบบ no-space (ลบ whitespace/newlines ทั้งหมด)
   - ไม่เอาค่าในเอกสารมา override (เพราะต้องตรงกันทุกครั้งตามนโยบายคุณ)
4) ✅ LOCK: L_description pattern (อย่างน้อย SHOPEE):
   Record Marketplace Expense - Shopee - Seller ID <<xxxx>> - <<Username>> - <<File Name>>
5) ✅ LOCK: K_account mapping ตามบริษัท (client_tax_id):
   - SHD (0105563022918) -> 520317
   - (RB/TOPONE ถ้ายังไม่สรุป ให้คงเดิม/ปล่อยว่าง)
6) ✅ Accounting Guards:
   - ห้ามเดา date จาก doc id / filename (เช่น 1203-xxxx) -> worker ไม่เดา
   - WHT ต้องคำนวณจาก Subtotal: worker ไม่คำนวณเอง แต่ “ไม่ล้าง P_wht”
   - ห้ามเอา WHT ไปใส่ N_unit_price
   - ห้ามเอา R_paid_amount ไปเติม N_unit_price แบบมั่ว (เอาไว้ที่ extractor/post_process เท่านั้น)
7) ✅ กัน AI ใส่ P_wht: AI patch จะ ignore key=P_wht เสมอ
8) ✅ Wallet mapping:
   - พยายาม map Q_payment_method เป็น EWLxxx ผ่าน wallet_mapping.py (ถ้ามี)
9) ✅ ไม่ทำให้ UI state=error / rows=0 ง่าย ๆ:
   - ถ้าอ่าน text ไม่ได้ → ใส่ 1 row NEEDS_REVIEW พร้อมเหตุผล
"""

from __future__ import annotations

import io
import os
import re
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

from .extract_service import extract_row_from_text
from .ocr_service import maybe_ocr_to_text
from .ai_service import ai_fill_peak_row
from ..utils.text_utils import normalize_text
from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

# ✅ Import from platform_constants (NO circular import!)
from .platform_constants import normalize_platform as _norm_platform

# ✅ wallet mapping
try:
    from ..extractors.wallet_mapping import resolve_wallet_code
except Exception:  # pragma: no cover
    resolve_wallet_code = None  # type: ignore


# ============================================================
# Company / Client config
# ============================================================

CLIENT_TAX_IDS: Dict[str, str] = {
    "RABBIT": "0105561071873",
    "SHD": "0105563022918",
    "TOPONE": "0105565027615",
}
TAXID_TO_COMPANY: Dict[str, str] = {v: k for k, v in CLIENT_TAX_IDS.items()}

# ✅ LOCK: account mapping (เติมเพิ่มได้ภายหลัง)
ACCOUNT_BY_CLIENT_TAX_ID: Dict[str, str] = {
    "0105563022918": "520317",  # SHD
    # "0105561071873": ".....",  # RABBIT (ใส่ตอนคุณยืนยันจากรูป)
    # "0105565027615": ".....",  # TOPONE (ใส่ตอนคุณยืนยันจากรูป)
}


# ============================================================
# Regex / helpers
# ============================================================

RE_ALL_WS = re.compile(r"\s+")
RE_TAX13_STRICT = re.compile(r"\b(\d{13})\b")

RE_SELLER_ID_HINTS = [
    re.compile(
        r"\b(?:seller_id|seller\s*id|shop_id|shop\s*id|merchant_id|merchant\s*id)\b\D{0,20}(\d{5,20})",
        re.IGNORECASE,
    ),
    re.compile(r"(?:รหัสร้าน|ไอดีร้าน|รหัสผู้ขาย|ร้านค้า)\D{0,20}(\d{5,20})", re.IGNORECASE),
]

RE_USERNAME_HINTS = [
    re.compile(r"\busername\b\D{0,20}([A-Za-z0-9_.\-]{2,64})", re.IGNORECASE),
    re.compile(r"(?:ชื่อผู้ใช้|ยูสเซอร์|ชื่อร้าน)\D{0,20}([A-Za-z0-9_.\-]{2,64})", re.IGNORECASE),
]

RE_ANY_LONG_DIGITS = re.compile(r"\b(\d{6,20})\b")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def _clean_money_str(v: Any) -> str:
    """
    Clean money string: remove currency symbols and commas.
    Keep decimals as-is (string) because downstream export expects string.
    """
    s = _safe_str(v)
    if not s:
        return ""
    s = s.replace("฿", "").replace("THB", "").replace(",", "").strip()
    # normalize weird spaces
    s = RE_ALL_WS.sub("", s)
    return s


def _compact_ref(v: Any) -> str:
    """Remove ALL whitespace/newlines inside reference/invoice strings."""
    s = _safe_str(v)
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _filename_base(filename: str) -> str:
    """Return basename with extension."""
    try:
        return os.path.basename(filename or "").strip()
    except Exception:
        return filename or ""


def _filename_stem(filename: str) -> str:
    """Return basename stem WITHOUT extension."""
    base = _filename_base(filename)
    if not base:
        return ""
    stem, _ext = os.path.splitext(base)
    return stem.strip()


def _doc_ref_from_filename(filename: str) -> str:
    """
    ✅ LOCK: C_reference / G_invoice_no ต้องเป็น doc+ref จากชื่อไฟล์เต็มๆ (stem)
    - normalize no-space
    Example:
      Shopee-TIV-TRSPEMKP00-00000-251201-0013100.pdf
      -> Shopee-TIV-TRSPEMKP00-00000-251201-0013100
    """
    stem = _filename_stem(filename)
    if not stem:
        return ""
    return _compact_ref(stem)


def _detect_platform_hint_from_filename(filename: str) -> str:
    """
    Quick filename hint (fallback only)
    Priority:
    - META/GOOGLE
    - SPX before SHOPEE
    - Marketplace
    - THAI_TAX
    """
    fn = (filename or "").upper()

    if "META" in fn or "FACEBOOK" in fn or "RCMETA" in fn:
        return "META"
    if "GOOGLE" in fn or "GOOG" in fn:
        return "GOOGLE"

    if "SPX" in fn or "RCSPX" in fn or "SHOPEE EXPRESS" in fn or "SHOPEE-EXPRESS" in fn:
        return "SPX"

    if "SHOPEE" in fn:
        return "SHOPEE"
    if "LAZADA" in fn or fn.startswith("LAZ"):
        return "LAZADA"
    if "TIKTOK" in fn or "TTS" in fn or "TTSHOP" in fn:
        return "TIKTOK"

    if "TAX" in fn or "INVOICE" in fn or "ใบกำกับ" in fn:
        return "THAI_TAX"

    return "UNKNOWN"


# ============================================================
# Client Detection
# ============================================================

def _detect_client_tax_id(text: str, filename: str = "", cfg: Optional[Dict[str, Any]] = None) -> str:
    """
    Priority:
      1) Text contains known client tax IDs (best)
      2) cfg has exactly one client_tax_ids (user selected)
      3) Filename contains RABBIT/SHD/TOPONE
    """
    t = text or ""
    for tax in CLIENT_TAX_IDS.values():
        if tax and tax in t:
            return tax

    if isinstance(cfg, dict):
        taxs = cfg.get("client_tax_ids")
        if isinstance(taxs, list) and len(taxs) == 1 and str(taxs[0]).strip():
            return str(taxs[0]).strip()

    fn = (filename or "").upper()
    for key, tax in CLIENT_TAX_IDS.items():
        if key in fn:
            return tax

    return ""


def _company_from_tax_id(client_tax_id: str, filename: str = "") -> str:
    if client_tax_id and client_tax_id in TAXID_TO_COMPANY:
        return TAXID_TO_COMPANY[client_tax_id]

    fn = (filename or "").upper()
    for k in ("RABBIT", "SHD", "TOPONE"):
        if k in fn:
            return k
    return ""


def _detect_seller_id(text: str, filename: str = "") -> str:
    t = text or ""
    for rx in RE_SELLER_ID_HINTS:
        m = rx.search(t)
        if m:
            return _safe_str(m.group(1))

    candidates = RE_ANY_LONG_DIGITS.findall(t)
    if candidates:
        return candidates[0]

    # fallback digits in filename
    fn_digits = re.findall(r"\d{6,20}", filename or "")
    if fn_digits:
        return fn_digits[0]

    return ""


def _detect_username(text: str) -> str:
    t = text or ""
    for rx in RE_USERNAME_HINTS:
        m = rx.search(t)
        if m:
            return _safe_str(m.group(1))
    return ""


# ============================================================
# Job Config Helpers
# ============================================================

def _get_job_cfg(job_service, job_id: str) -> Dict[str, Any]:
    """Read cfg dict from job service (backward compatible)."""
    try:
        job = job_service.get_job(job_id)  # type: ignore[attr-defined]
    except Exception:
        job = None

    if not isinstance(job, dict):
        return {}

    cfg = job.get("cfg")
    if isinstance(cfg, dict):
        return cfg

    # fallback legacy shapes
    filters = job.get("filters")
    if isinstance(filters, dict):
        out: Dict[str, Any] = {}
        for k in ("client_tax_ids", "client_tags", "platforms", "strictMode"):
            if k in filters:
                out[k] = filters.get(k)
        return out

    return {}


def _get_job_filters(job_service, job_id: str) -> Tuple[List[str], List[str], bool]:
    """
    Returns:
        (allowed_companies(tags), allowed_platforms, strictMode)
    """
    cfg = _get_job_cfg(job_service, job_id)
    strict_mode = bool(cfg.get("strictMode", False)) if isinstance(cfg, dict) else False

    companies = cfg.get("client_tags") if isinstance(cfg, dict) else []
    platforms = cfg.get("platforms") if isinstance(cfg, dict) else []

    def _norm_list(x: Any, *, kind: str) -> List[str]:
        if x is None:
            return []
        if isinstance(x, str):
            parts = [p.strip() for p in x.split(",") if p.strip()]
        elif isinstance(x, (list, tuple, set)):
            parts = [str(i).strip() for i in x if str(i).strip()]
        else:
            parts = []
        if kind == "company":
            out = [p.upper() for p in parts if p]
        else:
            out = [_norm_platform(p) or "UNKNOWN" for p in parts if p]
        # unique keep order
        seen = set()
        uniq: List[str] = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    return (_norm_list(companies, kind="company"), _norm_list(platforms, kind="platform"), strict_mode)


def _cfg_mismatch(
    allowed_companies: List[str],
    allowed_platforms: List[str],
    strict_mode: bool,
    *,
    company: str,
    platform_u: str,
) -> Tuple[bool, str]:
    """
    If filter exists and mismatch -> NEEDS_REVIEW.
    If strictMode and cannot detect -> NEEDS_REVIEW.
    """
    c = (company or "").upper().strip()
    p = (platform_u or "UNKNOWN").upper().strip()

    has_company_filter = bool(allowed_companies)
    has_platform_filter = bool(allowed_platforms)

    if not has_company_filter and not has_platform_filter:
        return (False, "")

    # company
    if has_company_filter:
        if c:
            if c not in allowed_companies:
                return (True, f"company={c} not in allowed ({','.join(allowed_companies)})")
        else:
            if strict_mode:
                return (True, f"company=unknown (strictMode, allowed: {','.join(allowed_companies)})")

    # platform
    if has_platform_filter:
        if p and p != "UNKNOWN":
            if p not in allowed_platforms:
                return (True, f"platform={p} not in allowed ({','.join(allowed_platforms)})")
        else:
            if strict_mode:
                return (True, f"platform=unknown (strictMode, allowed: {','.join(allowed_platforms)})")

    return (False, "")


# ============================================================
# Validation
# ============================================================

def _revalidate(row: Dict[str, Any]) -> List[str]:
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
# Row normalization (GLOBAL POLICIES HERE)
# ============================================================

def _apply_locked_fields(row: Dict[str, Any], *, filename: str, platform_u: str, text: str, client_tax_id: str) -> None:
    """
    ✅ Single place to LOCK these policies:
    - C_reference == G_invoice_no == doc+ref from filename stem (no-space)
    - K_account (client-based)
    - L_description pattern (platform-based)
    """
    # ---- LOCK doc+ref ----
    doc_ref = _doc_ref_from_filename(filename)
    if doc_ref:
        row["C_reference"] = doc_ref
        row["G_invoice_no"] = doc_ref
    else:
        # still force equality if something exists
        c = _compact_ref(row.get("C_reference"))
        g = _compact_ref(row.get("G_invoice_no"))
        pick = c or g
        if pick:
            row["C_reference"] = pick
            row["G_invoice_no"] = pick

    # ---- LOCK K_account by client (SHD required) ----
    if client_tax_id and client_tax_id in ACCOUNT_BY_CLIENT_TAX_ID:
        row["K_account"] = ACCOUNT_BY_CLIENT_TAX_ID[client_tax_id]

    # ---- LOCK L_description pattern ----
    base = _filename_base(filename)
    seller_id = _safe_str(row.get("_seller_id")) or _detect_seller_id(text, filename)
    username = _detect_username(text)

    # Fallbacks (ให้ไม่ว่าง)
    seller_id = seller_id or "unknown"
    username = username or "unknown"

    # Only override when empty OR looks generic/garbled
    cur_desc = _safe_str(row.get("L_description"))
    should_override = (not cur_desc) or (cur_desc.lower() in {"-", "n/a", "na", "unknown"})

    if platform_u == "SHOPEE":
        locked_desc = f"Record Marketplace Expense - Shopee - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "LAZADA":
        locked_desc = f"Record Marketplace Expense - Lazada - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "TIKTOK":
        locked_desc = f"Record Marketplace Expense - TikTok - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "SPX":
        locked_desc = f"Record Delivery/Logistics Expense - SPX - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "META":
        locked_desc = f"Record Advertising Expense - Meta - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "GOOGLE":
        locked_desc = f"Record Advertising Expense - Google - {base}"
        if should_override:
            row["L_description"] = locked_desc
    else:
        # UNKNOWN/THAI_TAX: keep user's/extractor's desc if any
        if should_override:
            row["L_description"] = f"Record Expense - {platform_u} - {base}"


def _normalize_row_fields(row: Dict[str, Any], seq: int) -> None:
    """
    Normalize & enforce generic policies.
    NOTE:
    - worker จะไม่เดา date จาก filename
    - worker จะไม่คำนวณ WHT (ต้องทำใน extractor จาก Subtotal)
    - worker จะไม่ล้าง P_wht ทิ้งอีกต่อไป
    - worker จะไม่ cross-fill N_unit_price จาก R_paid_amount แบบเสี่ยง
    """
    # ✅ Running seq
    row["A_seq"] = seq

    # dates -> digits only (YYYYMMDD) (แต่ไม่เดาจาก filename)
    for k in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        if row.get(k):
            row[k] = _digits_only(_safe_str(row.get(k)))[:8]
        else:
            row[k] = _safe_str(row.get(k))

    # tax id / branch
    row["E_tax_id_13"] = _digits_only(_safe_str(row.get("E_tax_id_13")))[:13]
    br = _digits_only(_safe_str(row.get("F_branch_5")))
    row["F_branch_5"] = br.zfill(5)[:5] if br else "00000"

    # price_type
    j = _safe_str(row.get("J_price_type"))
    row["J_price_type"] = j if j in {"1", "2", "3"} else (j or "1")

    # vat rate
    o = _safe_str(row.get("O_vat_rate")).upper()
    row["O_vat_rate"] = "NO" if o in {"NO", "0", "NONE"} else ("7%" if (o == "" or "7" in o) else o)

    # qty default
    row["M_qty"] = _safe_str(row.get("M_qty") or "1") or "1"

    # ✅ Money: clean only, DO NOT mix fields
    # N_unit_price = keep as-is (clean), if empty -> "0"
    n = _clean_money_str(row.get("N_unit_price"))
    row["N_unit_price"] = n if n else "0"

    # R_paid_amount = keep as-is (clean), if empty -> "0"
    r = _clean_money_str(row.get("R_paid_amount"))
    row["R_paid_amount"] = r if r else "0"

    # ✅ P_wht: keep if extractor computed (but clean). Do NOT force empty.
    # (AI patch จะถูกห้ามไม่ให้ใส่ P_wht อยู่แล้ว)
    p = _clean_money_str(row.get("P_wht"))
    row["P_wht"] = p  # may be "" if none

    # ✅ compact ref/invoice (remove whitespace/newlines)
    row["C_reference"] = _compact_ref(row.get("C_reference"))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no"))

    # normalize common text fields
    for k in (
        "A_company_name",
        "D_vendor_code",
        "K_account",
        "L_description",
        "Q_payment_method",
        "S_pnd",
        "T_note",
        "U_group",
        "_source_file",
        "_platform",
        "_client_tax_id",
        "_seller_id",
        "_status",
        "_status_reason",
    ):
        if k in row:
            row[k] = _safe_str(row.get(k))


# ============================================================
# PDF/OCR Helpers
# ============================================================

def _extract_embedded_pdf_text(data: bytes, max_pages: int = 15) -> str:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            parts: List[str] = []
            for p in pdf.pages[:max_pages]:
                parts.append(p.extract_text() or "")
            return "\n".join(parts).strip()
    except Exception:
        return ""


def _write_temp_file(filename: str, data: bytes) -> str:
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}:
        # detect content
        if data[:5] == b"%PDF-":
            ext = ".pdf"
        else:
            ext = ext or ".bin"

    fd, path = tempfile.mkstemp(prefix="peak_import_", suffix=ext)
    os.close(fd)
    with open(path, "wb") as f:
        f.write(data)
    return path


def _should_call_ai(errors: List[str], row: Dict[str, Any]) -> bool:
    critical_missing = (
        not _safe_str(row.get("B_doc_date"))
        or not _safe_str(row.get("L_description"))
        or _safe_str(row.get("R_paid_amount")) in {"", "0", "0.0", "0.00"}
    )
    return bool(errors) or critical_missing


def _append_and_update_file(
    job_service,
    job_id: str,
    idx: int,
    *,
    rows: List[Dict[str, Any]],
    state: str,
    platform: str,
    company: str,
    message: str,
) -> None:
    # append rows first (so UI sees them)
    if rows:
        job_service.append_rows(job_id, rows)
    # update file status
    job_service.update_file(
        job_id,
        idx,
        {
            "state": state,
            "platform": platform,
            "company": company,
            "message": message,
            "rows_count": len(rows),
        },
    )


def _merge_unique_errors(*lists: List[str]) -> List[str]:
    out: List[str] = []
    for xs in lists:
        for e in xs or []:
            if e and e not in out:
                out.append(e)
    return out


def _add_note(row: Dict[str, Any], note: str) -> None:
    note = _safe_str(note)
    if not note:
        return
    cur = _safe_str(row.get("T_note"))
    if not cur:
        row["T_note"] = note
    else:
        if note not in cur:
            row["T_note"] = f"{cur} | {note}"


# ============================================================
# Main worker
# ============================================================

def process_job_files(job_service, job_id: str) -> None:
    """
    Worker that:
    - extracts text (pdfplumber -> OCR fallback)
    - routes to extractor (extract_service)
    - optionally calls AI to patch missing/invalid fields
    - enforces LOCKED policies (doc+ref + description + account)
    - enforces backend filters
    - produces running A_seq across whole job
    """
    payloads: List[Tuple[str, str, bytes]] = job_service.get_payloads(job_id)

    allowed_companies, allowed_platforms, strict_mode = _get_job_filters(job_service, job_id)
    cfg = _get_job_cfg(job_service, job_id)

    # ✅ Running sequence across the whole job
    seq = 1

    ok_files = 0
    review_files = 0
    error_files = 0
    processed = 0

    ai_only_fill_empty = _env_bool("AI_ONLY_FILL_EMPTY", default=False)

    for idx, (filename, content_type, data) in enumerate(payloads):
        filename = filename or "unknown"
        content_type = content_type or ""

        job_service.update_file(job_id, idx, {"state": "processing"})

        platform_u = "UNKNOWN"
        company = ""
        rows_out: List[Dict[str, Any]] = []
        tmp_path: Optional[str] = None

        try:
            # ---------- Extract text ----------
            text = ""
            is_pdf = filename.lower().endswith(".pdf") or (content_type == "application/pdf")

            if is_pdf:
                text = _extract_embedded_pdf_text(data, max_pages=15)

            if not text:
                tmp_path = _write_temp_file(filename, data)
                text = maybe_ocr_to_text(tmp_path)

            text = normalize_text(text)

            client_tax_id = _detect_client_tax_id(text, filename, cfg=cfg)
            company = _company_from_tax_id(client_tax_id, filename)

            # ---------- If still no text ----------
            if not text:
                platform_u = _norm_platform(_detect_platform_hint_from_filename(filename)) or "UNKNOWN"

                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )

                row_min: Dict[str, Any] = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_client_tax_id": client_tax_id,
                    "_status": "NEEDS_REVIEW",
                    "_status_reason": "no_text",
                    "_errors": ["ไม่พบข้อความจากเอกสาร"],
                }

                # normalize + LOCK fields (doc+ref, desc, account)
                _normalize_row_fields(row_min, seq=seq)
                _apply_locked_fields(row_min, filename=filename, platform_u=platform_u, text="", client_tax_id=client_tax_id)

                if is_mismatch:
                    row_min["_errors"] = list(row_min.get("_errors") or []) + [f"ไม่ตรง filter: {mismatch_reason}"]
                    row_min["_status_reason"] = "filter_mismatch"
                    _add_note(row_min, f"Filtered: {mismatch_reason}")

                rows_out.append(row_min)
                seq += 1

                file_state = "needs_review"
                message = "ไม่พบข้อความจากเอกสาร"
                if is_mismatch:
                    message += f" | {mismatch_reason}"
                review_files += 1

                _append_and_update_file(
                    job_service,
                    job_id,
                    idx,
                    rows=rows_out,
                    state=file_state,
                    platform=platform_u,
                    company=company,
                    message=message,
                )

            else:
                # ---------- Extract structured row ----------
                platform, base_row, errors = extract_row_from_text(
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                )
                platform_u = _norm_platform(platform) or "UNKNOWN"

                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )

                seller_id = _detect_seller_id(text, filename)
                shop_name_hint = _filename_stem(filename)

                wallet_code = ""
                if resolve_wallet_code is not None:
                    try:
                        wallet_code = (
                            resolve_wallet_code(
                                client_tax_id,
                                seller_id=seller_id,
                                shop_name=shop_name_hint,
                                text=text,
                            )
                            or ""
                        )
                    except Exception:
                        wallet_code = ""

                row: Dict[str, Any] = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_client_tax_id": client_tax_id,
                    "_seller_id": seller_id,
                    "_errors": list(errors) if errors else [],
                }
                if isinstance(base_row, dict):
                    row.update(base_row)

                # wallet
                if wallet_code:
                    row["Q_payment_method"] = wallet_code

                # Normalize first (clean)
                _normalize_row_fields(row, seq=seq)

                # ✅ LOCK fields (doc+ref, desc, account) BEFORE AI
                _apply_locked_fields(row, filename=filename, platform_u=platform_u, text=text, client_tax_id=client_tax_id)

                # ---------- Optional AI patch ----------
                if _should_call_ai(list(row.get("_errors") or []), row):
                    partial_keys = [
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

                    ai_patch = ai_fill_peak_row(
                        text=text,
                        platform_hint=platform_u,
                        partial_row={k: row.get(k, "") for k in partial_keys},
                        source_filename=filename,
                    )

                    if ai_patch and isinstance(ai_patch, dict):
                        for k, v in ai_patch.items():
                            if not k:
                                continue
                            if k.startswith("_"):
                                row[k] = v
                                continue

                            # ✅ HARD LOCK: AI ห้ามใส่ P_wht
                            if k == "P_wht":
                                continue

                            v_str = _safe_str(v)
                            if not v_str:
                                continue

                            if ai_only_fill_empty:
                                if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                    row[k] = v_str
                            else:
                                if row.get("_errors"):
                                    row[k] = v_str
                                else:
                                    if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                        row[k] = v_str

                # re-apply wallet (wallet wins)
                if wallet_code:
                    row["Q_payment_method"] = wallet_code

                # ✅ normalize again
                _normalize_row_fields(row, seq=seq)

                # ✅ re-lock again (AI ห้ามทำให้ ref/desc/account เพี้ยน)
                _apply_locked_fields(row, filename=filename, platform_u=platform_u, text=text, client_tax_id=client_tax_id)

                # ---------- Validation ----------
                errors2 = _revalidate(row)
                row["_errors"] = _merge_unique_errors(list(row.get("_errors") or []), errors2)

                # ---------- Backend filtering result ----------
                if is_mismatch:
                    row["_status"] = "NEEDS_REVIEW"
                    row["_status_reason"] = "filter_mismatch"
                    row["_errors"] = _merge_unique_errors(list(row.get("_errors") or []), [f"ไม่ตรง filter: {mismatch_reason}"])
                    _add_note(row, f"Filtered: {mismatch_reason}")
                    file_state = "needs_review"
                    message = mismatch_reason
                    review_files += 1
                else:
                    if row.get("_errors"):
                        row["_status"] = "NEEDS_REVIEW"
                        row["_status_reason"] = "validation_or_missing"
                        file_state = "needs_review"
                        message = "มีช่องที่ต้องตรวจสอบ"
                        review_files += 1
                    else:
                        row["_status"] = "OK"
                        row["_status_reason"] = ""
                        file_state = "done"
                        message = ""
                        ok_files += 1

                rows_out.append(row)
                seq += 1

                _append_and_update_file(
                    job_service,
                    job_id,
                    idx,
                    rows=rows_out,
                    state=file_state,
                    platform=platform_u,
                    company=company,
                    message=message,
                )

        except Exception as e:
            error_files += 1

            err_row: Dict[str, Any] = {
                "A_seq": seq,
                "A_company_name": company or "",
                "_source_file": filename,
                "_platform": platform_u or "UNKNOWN",
                "_status": "ERROR",
                "_status_reason": "exception",
                "_errors": [f"{type(e).__name__}: {e}"],
            }
            _normalize_row_fields(err_row, seq=seq)
            _apply_locked_fields(err_row, filename=filename, platform_u=platform_u or "UNKNOWN", text="", client_tax_id="")
            seq += 1

            try:
                job_service.append_rows(job_id, [err_row])
            except Exception:
                pass

            job_service.update_file(
                job_id,
                idx,
                {
                    "state": "error",
                    "platform": platform_u or "UNKNOWN",
                    "company": company or "",
                    "message": f"Error: {type(e).__name__}: {e}",
                    "rows_count": 1,
                },
            )

        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        processed += 1
        job_service.update_job(
            job_id,
            {
                "processed_files": processed,
                "ok_files": ok_files,
                "review_files": review_files,
                "error_files": error_files,
            },
        )

    final_state = "done" if error_files == 0 else "error"
    job_service.update_job(job_id, {"state": final_state})


__all__ = ["process_job_files"]
