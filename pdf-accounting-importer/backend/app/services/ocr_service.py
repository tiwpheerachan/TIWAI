# backend/app/services/ocr_service.py
from __future__ import annotations

import io
import logging
import os
from typing import List, Tuple, Optional

from PIL import Image

logger = logging.getLogger(__name__)


# -------------------------
# Helpers
# -------------------------
def _is_pdf(path: str) -> bool:
    return str(path).lower().endswith(".pdf")


def _is_image(path: str) -> bool:
    p = str(path).lower()
    return p.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"))


def _safe_int(env_name: str, default: int) -> int:
    try:
        return int(os.getenv(env_name, str(default)))
    except Exception:
        return default


def _safe_float(env_name: str, default: float) -> float:
    try:
        return float(os.getenv(env_name, str(default)))
    except Exception:
        return default


def _safe_str(env_name: str, default: str) -> str:
    try:
        v = os.getenv(env_name, default)
        return str(v).strip()
    except Exception:
        return default


def _pdf_has_text_fast(pdf_path: str, min_chars: int = 80) -> Tuple[bool, str]:
    """
    ถ้า PDF เป็นตัวอักษรจริง (ไม่ใช่สแกน) จะดึง text ได้เลย (เร็วและฟรี)
    - ใช้ PyMuPDF (fitz)
    """
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(pdf_path)
        texts: List[str] = []
        for i in range(min(doc.page_count, 3)):  # เช็คแค่ 1-3 หน้าแรกพอ
            t = doc.load_page(i).get_text("text") or ""
            texts.append(t)
        joined = "\n".join(texts).strip()
        return (len(joined) >= min_chars, joined)
    except ModuleNotFoundError:
        logger.warning("PyMuPDF (fitz) not installed. Cannot do fast PDF text extraction.")
        return (False, "")
    except Exception as e:
        logger.warning("pdf_has_text_fast failed: %s", e)
        return (False, "")


def _render_pdf_to_images(pdf_path: str, max_pages: int = 20, zoom: float = 2.0) -> List[Image.Image]:
    """
    Render PDF -> PIL images ด้วย PyMuPDF (ไม่ต้องติด poppler)
    """
    try:
        import fitz  # PyMuPDF
    except ModuleNotFoundError as e:
        raise RuntimeError("Missing dependency: PyMuPDF. Install with: pip install pymupdf") from e

    doc = fitz.open(pdf_path)
    imgs: List[Image.Image] = []

    n = min(doc.page_count, max_pages)
    mat = fitz.Matrix(zoom, zoom)

    for i in range(n):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        imgs.append(img)

    return imgs


def _open_image_safe(file_path: str) -> Optional[Image.Image]:
    """
    Open image file safely as RGB.
    """
    try:
        img = Image.open(file_path)
        # Ensure loaded
        img.load()
        return img.convert("RGB")
    except Exception as e:
        logger.warning("open image failed: %s", e)
        return None


# -------------------------
# OCR Service
# -------------------------
class OCRService:
    """
    OCR pipeline (ฉลาด + deploy-friendly):

    ✅ Always try "fast text extraction" for PDF (ไม่ถือเป็น OCR) — ทำงานได้แม้ปิด OCR
    - PDF with text layer: extract via PyMuPDF (fast, free)

    ✅ OCR (optional) สำหรับ PDF สแกน/รูปภาพ:
    - scanned PDF: render to images -> OCR via PaddleOCR (ถ้ามี)
    - image: OCR via PaddleOCR (ถ้ามี)

    ✅ Safe defaults:
    - Default OCR_PROVIDER = "none" (กัน Render free พังจาก paddle/pymupdf build issues)
    - Enable OCR only when explicitly configured and dependencies exist
    """

    def __init__(self):
        # IMPORTANT: แยก "fast PDF text extraction" ออกจาก OCR
        # OCR_ENABLE: เปิด/ปิดเฉพาะการ OCR ภาพ/สแกน
        self.enable_ocr: bool = _safe_str("ENABLE_OCR", "1") == "1"

        # Default provider = none เพื่อ deploy ผ่านง่าย (คุณจะเปิดเองตอนพร้อม)
        # paddle|document_ai|none
        self.provider: str = _safe_str("OCR_PROVIDER", "none").lower()

        # PDF fast-text threshold (ถ้าดึงได้เกินนี้ ถือว่าเป็น text layer)
        self.min_chars: int = _safe_int("OCR_MIN_TEXT_CHARS", 80)

        # Render PDF -> images settings (ใช้เมื่อจำเป็นต้อง OCR)
        self.max_pages: int = _safe_int("OCR_MAX_PAGES", 20)
        self.zoom: float = _safe_float("OCR_PDF_ZOOM", 2.0)

        # Paddle options (ถ้าเปิด)
        self.paddle_lang: str = _safe_str("PADDLE_OCR_LANG", "en")

        self._paddle = None
        self._paddle_ready = False

        # init provider lazily (only when needed)
        # แต่ถ้าตั้งใจเปิด paddle ก็พยายาม init ให้ก่อน
        if self.enable_ocr and self.provider == "paddle":
            self._init_paddle()

    # -------------------------
    # Provider init (lazy & safe)
    # -------------------------
    def _init_paddle(self) -> None:
        """
        Initialize PaddleOCR safely.
        - ถ้าไม่มี dependency -> ไม่ล้ม, แค่ปิด OCR ภาพ/สแกน
        """
        if self._paddle_ready:
            return

        try:
            from paddleocr import PaddleOCR

            self._paddle = PaddleOCR(
                use_angle_cls=True,
                lang=self.paddle_lang,
                show_log=False,
            )
            self._paddle_ready = True
            logger.info("PaddleOCR initialized (lang=%s)", self.paddle_lang)
        except ModuleNotFoundError:
            logger.warning("paddleocr not installed. OCR for images/scanned PDFs will be disabled.")
            self._paddle_ready = False
        except Exception as e:
            logger.warning("Failed to init PaddleOCR: %s", e)
            self._paddle_ready = False

    def _ensure_provider_ready(self) -> None:
        """
        Ensure OCR provider is ready (only for OCR part).
        """
        if not self.enable_ocr:
            return

        if self.provider == "paddle":
            self._init_paddle()

    # -------------------------
    # Public API
    # -------------------------
    def extract_text(self, file_path: str) -> str:
        """
        ฟังก์ชันหลักที่แนะนำให้ใช้:
        - PDF: พยายามดึง text layer ก่อนเสมอ (ฟรี/เร็ว)
        - ถ้าเป็นสแกน/รูป: OCR เฉพาะเมื่อ enable_ocr และ provider พร้อม
        """
        if not file_path:
            return ""

        # 1) PDF: always try fast extract (works even when OCR disabled)
        if _is_pdf(file_path):
            has_text, text = _pdf_has_text_fast(file_path, min_chars=self.min_chars)
            if has_text:
                return text

            # PDF สแกน → OCR (ถ้าเปิดและพร้อม)
            return self._ocr_scanned_pdf(file_path)

        # 2) Image: OCR (ถ้าเปิดและพร้อม)
        if _is_image(file_path):
            return self._ocr_image(file_path)

        # 3) Unknown type
        logger.warning("Unsupported file type for OCR/text extraction: %s", file_path)
        return ""

    # -------------------------
    # OCR implementations
    # -------------------------
    def _ocr_scanned_pdf(self, pdf_path: str) -> str:
        """
        OCR scanned PDF (render -> OCR)
        """
        # ถ้าปิด OCR หรือ provider none → return empty
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return ""

        if self.provider == "document_ai":
            return self._ocr_document_ai(pdf_path)

        # default: paddle
        self._ensure_provider_ready()
        if not self._paddle_ready:
            return ""

        try:
            pages = _render_pdf_to_images(pdf_path, max_pages=self.max_pages, zoom=self.zoom)
        except Exception as e:
            logger.warning("render_pdf_to_images failed: %s", e)
            return ""

        return self._ocr_images_with_paddle(pages)

    def _ocr_image(self, file_path: str) -> str:
        """
        OCR image file
        """
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return ""

        if self.provider == "document_ai":
            return self._ocr_document_ai(file_path)

        # default: paddle
        self._ensure_provider_ready()
        if not self._paddle_ready:
            return ""

        img = _open_image_safe(file_path)
        if img is None:
            return ""

        return self._ocr_images_with_paddle([img])

    def _ocr_images_with_paddle(self, images: List[Image.Image]) -> str:
        """
        Run PaddleOCR on list of PIL images.
        """
        if not self._paddle_ready or not self._paddle:
            return ""

        try:
            import numpy as np
        except ModuleNotFoundError:
            logger.warning("numpy not installed. Cannot run PaddleOCR.")
            return ""

        chunks: List[str] = []

        for img in images:
            try:
                arr = np.array(img)
                result = self._paddle.ocr(arr, cls=True) or []
            except Exception as e:
                logger.warning("PaddleOCR failed on an image: %s", e)
                continue

            # result format: [ [ [box], (text, score) ], ... ] per line
            for line in result:
                for item in line:
                    try:
                        text_score = item[1]
                        if isinstance(text_score, (list, tuple)) and len(text_score) >= 1:
                            txt = text_score[0]
                            if txt:
                                chunks.append(str(txt))
                    except Exception:
                        continue

        return "\n".join(chunks).strip()

    def _ocr_document_ai(self, file_path: str) -> str:
        """
        Placeholder for Document AI (future).
        """
        raise NotImplementedError("document_ai not enabled in this build yet")


# -------------------------
# Compatibility helper (kept name)
# -------------------------
def maybe_ocr_to_text(file_path: str) -> str:
    """
    backward-compatible helper for job_worker.py

    ✅ behavior (improved):
    - PDF มี text layer: extract ได้เสมอ (แม้ปิด OCR)
    - PDF สแกน/รูป: OCR เฉพาะเมื่อเปิด ENABLE_OCR=1 และ OCR_PROVIDER=paddle/document_ai
    """
    try:
        return OCRService().extract_text(file_path)
    except Exception as e:
        logger.warning("maybe_ocr_to_text failed: %s", e)
        return ""
