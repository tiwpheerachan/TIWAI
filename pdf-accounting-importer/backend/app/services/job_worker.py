# backend/app/services/job_worker.py
from __future__ import annotations

import io
import os
import re
import tempfile
from typing import List, Dict, Any, Tuple

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

# -------------------------
# Config / small helpers
# -------------------------

CLIENT_TAX_IDS = {
    "RABBIT": "0105561071873",
    "SHD": "0105563022918",
    "TOPONE": "0105565027615",
}

RE_TAX13_STRICT = re.compile(r"\b(\d{13})\b")
RE_THAI_DATE_HINT = re.compile(r"(?:วันที่|Date)\s*[:#：]?\s*([0-9]{1,2}[\/\-.][0-9]{1,2}[\/\-.][0-9]{2,4})", re.IGNORECASE)


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
    Keep as string for export. Tries to normalize commas/฿.
    Does NOT force to 2 decimals here (export_service handles text format).
    """
    s = _safe_str(v)
    if not s:
        return ""
    s = s.replace("฿", "").replace("THB", "").replace(",", "").strip()
    # allow "3%" for P_wht
    return s


def _detect_client_tax_id(text: str, filename: str = "") -> str:
    """
    Detect which client company this document belongs to.
    Priority:
      1) Text contains one of known client tax IDs
      2) Filename/path hints (RABBIT/SHD/TOPONE)
    """
    t = text or ""
    for _, tax in CLIENT_TAX_IDS.items():
        if tax in t:
            return tax

    fn = (filename or "").upper()
    for key, tax in CLIENT_TAX_IDS.items():
        if key in fn:
            return tax

    return ""


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


def _normalize_row_fields(row: Dict[str, Any]) -> None:
    """
    Normalize core PEAK fields after extractor + AI merge.
    Keeps everything as strings.
    """
    # Keep meta
    row["A_seq"] = int(row.get("A_seq") or 1)

    # Dates: must be YYYYMMDD or empty (validators will catch)
    for k in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        row[k] = _digits_only(_safe_str(row.get(k)))[:8] if row.get(k) else _safe_str(row.get(k))

    # Tax / branch
    row["E_tax_id_13"] = _digits_only(_safe_str(row.get("E_tax_id_13")))[:13]
    br = _digits_only(_safe_str(row.get("F_branch_5")))
    row["F_branch_5"] = br.zfill(5)[:5] if br else "00000"

    # price_type
    j = _safe_str(row.get("J_price_type"))
    row["J_price_type"] = j if j in {"1", "2", "3"} else (j or "1")

    # vat_rate
    o = _safe_str(row.get("O_vat_rate")).upper()
    row["O_vat_rate"] = "NO" if o in {"NO", "0", "NONE"} else ("7%" if (o == "" or "7" in o) else o)

    # qty
    row["M_qty"] = _safe_str(row.get("M_qty") or "1") or "1"

    # money-ish
    row["N_unit_price"] = _clean_money_str(row.get("N_unit_price") or row.get("R_paid_amount") or "0") or "0"
    row["R_paid_amount"] = _clean_money_str(row.get("R_paid_amount") or row.get("N_unit_price") or "0") or "0"

    # WHT: allow "3%" or numeric
    p = _safe_str(row.get("P_wht"))
    row["P_wht"] = p if "%" in p else (_clean_money_str(p) or "0")

    # strings
    for k in (
        "C_reference",
        "D_vendor_code",
        "G_invoice_no",
        "K_account",
        "L_description",
        "Q_payment_method",
        "S_pnd",
        "T_note",
        "U_group",
    ):
        row[k] = _safe_str(row.get(k))


def _extract_embedded_pdf_text(data: bytes, max_pages: int = 15) -> str:
    """
    PDF -> embedded text via pdfplumber (fast). If scanned, will usually return empty.
    """
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages: List[str] = []
            for p in pdf.pages[:max_pages]:
                pages.append(p.extract_text() or "")
            return "\n".join(pages).strip()
    except Exception:
        return ""


def _write_temp_file(filename: str, data: bytes) -> str:
    """
    Save uploaded bytes to a temp file (keeps extension if possible),
    so OCR pipeline that expects a file path can work.
    """
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}:
        # best guess: pdf if bytes look like PDF
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
    """
    Decide whether to call AI.
    - If we already have complete & valid core fields: skip.
    - If missing critical fields or validation errors: call.
    """
    critical_missing = (
        not _safe_str(row.get("B_doc_date"))
        or not _safe_str(row.get("L_description"))
        or _safe_str(row.get("R_paid_amount")) in {"", "0", "0.0", "0.00"}
    )
    return bool(errors) or critical_missing


# -------------------------
# Main worker
# -------------------------

def process_job_files(job_service, job_id: str) -> None:
    """
    Robust job processor:
    - PDF: try embedded text first (pdfplumber)
    - If empty: write temp file and run OCR/text extraction via ocr_service.maybe_ocr_to_text(file_path)
    - Extract base row via extract_service
    - Optional AI fill via ai_service (ENABLE_LLM=1)
    - Re-validate and mark status
    - Append rows incrementally
    """
    payloads: List[Tuple[str, str, bytes]] = job_service.get_payloads(job_id)

    seq = 1
    ok_files = 0
    review_files = 0
    error_files = 0
    processed = 0

    # AI merge policy:
    # - default: only fill empty fields, but if base extractor has errors, allow AI to override empties AND conflicting fields
    # - can force "AI only fill empty" by setting AI_ONLY_FILL_EMPTY=1
    ai_only_fill_empty = _env_bool("AI_ONLY_FILL_EMPTY", default=False)

    for idx, (filename, content_type, data) in enumerate(payloads):
        job_service.update_file(job_id, idx, {"state": "processing"})

        platform = "unknown"
        file_state = "done"
        message = ""
        rows_out: List[Dict[str, Any]] = []

        tmp_path: str | None = None

        try:
            text = ""
            is_pdf = (filename or "").lower().endswith(".pdf") or (content_type == "application/pdf")

            # 1) embedded PDF text first
            if is_pdf:
                text = _extract_embedded_pdf_text(data, max_pages=15)

            # 2) OCR/text extraction fallback (expects file_path)
            if not text:
                tmp_path = _write_temp_file(filename, data)
                text = maybe_ocr_to_text(tmp_path)  # <-- matches your latest ocr_service.py

            text = normalize_text(text)

            if not text:
                platform = "unknown"
                file_state = "needs_review"
                message = "ยังไม่มีข้อความจากเอกสาร (PDF สแกน/รูปภาพ) — ต้องเปิด OCR หรือรีวิวเอง"
                review_files += 1

                # still append a minimal row for traceability (optional)
                row_min = {
                    "A_seq": seq,
                    "_source_file": filename,
                    "_platform": platform,
                    "_status": "NEEDS_REVIEW",
                    "_errors": ["ไม่พบข้อความจากเอกสาร"],
                }
                rows_out.append(row_min)
                seq += 1

            else:
                # 3) Extract with your rule-based extractor
                platform, base_row, errors = extract_row_from_text(text)

                # detect which client company (Rabbit/SHD/TopOne) to help vendor mapping in extractors
                client_tax_id = _detect_client_tax_id(text, filename)
                # keep it in meta for debugging
                row: Dict[str, Any] = {
                    "A_seq": seq,
                    "_source_file": filename,
                    "_platform": platform,
                    "_client_tax_id": client_tax_id,
                    "_errors": list(errors) if errors else [],
                }
                row.update(base_row or {})

                # Normalize BEFORE AI (so AI sees cleaner partial_row)
                _normalize_row_fields(row)

                # 4) Optional AI completion step
                if _should_call_ai(row["_errors"], row):
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
                        platform_hint=platform,
                        partial_row={k: row.get(k, "") for k in partial_keys},
                        source_filename=filename,
                    )

                    if ai_patch:
                        # Merge AI results into row
                        for k, v in ai_patch.items():
                            if k.startswith("_"):
                                row[k] = v
                                continue

                            v_str = _safe_str(v)
                            if not v_str:
                                continue

                            if ai_only_fill_empty:
                                # only fill empty
                                if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                    row[k] = v_str
                            else:
                                # default:
                                # - if base extractor had errors -> allow AI override
                                # - else fill only empty
                                if row["_errors"]:
                                    row[k] = v_str
                                else:
                                    if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                        row[k] = v_str

                        # Helpful note when tax id missing (your requirement)
                        if not _safe_str(row.get("E_tax_id_13")):
                            note = (_safe_str(row.get("T_note"))).strip()
                            hint = "e-Tax Ready = NO (ไม่พบเลขผู้เสียภาษี 13 หลัก)"
                            if hint not in note:
                                row["T_note"] = (note + (" | " if note else "") + hint).strip()

                # Normalize AFTER AI merge
                _normalize_row_fields(row)

                # 5) Re-validate
                errors2 = _revalidate(row)
                row["_errors"] = errors2

                # 6) Status
                if errors2:
                    row["_status"] = "NEEDS_REVIEW"
                    file_state = "needs_review"
                    if row.get("_ai_confidence") is not None:
                        message = "มีช่องที่ต้องตรวจสอบ (ใช้ AI แล้ว)"
                    else:
                        message = "มีช่องที่ต้องตรวจสอบ"
                    review_files += 1
                else:
                    row["_status"] = "OK"
                    ok_files += 1

                rows_out.append(row)
                seq += 1

            # 7) Persist results
            job_service.append_rows(job_id, rows_out)
            job_service.update_file(
                job_id,
                idx,
                {
                    "state": file_state,
                    "platform": platform,
                    "message": message,
                    "rows_count": len(rows_out),
                },
            )

        except Exception as e:
            error_files += 1
            job_service.update_file(
                job_id,
                idx,
                {
                    "state": "error",
                    "platform": platform,
                    "message": f"Error: {type(e).__name__}: {e}",
                    "rows_count": 0,
                },
            )

        finally:
            # cleanup temp file
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
