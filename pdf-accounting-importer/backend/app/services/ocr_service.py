# backend/app/services/ocr_service.py
from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any

from PIL import Image

logger = logging.getLogger(__name__)

# ============================================================
# File type helpers
# ============================================================
def _is_pdf(path: str) -> bool:
    return str(path).lower().endswith(".pdf")


def _is_image(path: str) -> bool:
    p = str(path).lower()
    return p.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"))


def _safe_int(env_name: str, default: int) -> int:
    try:
        return int(str(os.getenv(env_name, str(default))).strip())
    except Exception:
        return default


def _safe_float(env_name: str, default: float) -> float:
    try:
        return float(str(os.getenv(env_name, str(default))).strip())
    except Exception:
        return default


def _safe_str(env_name: str, default: str) -> str:
    try:
        v = os.getenv(env_name, default)
        return str(v).strip()
    except Exception:
        return default


def _env_bool(env_name: str, default: bool = False) -> bool:
    v = os.getenv(env_name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


# ============================================================
# Image helpers
# ============================================================
def _open_image_safe(file_path: str) -> Optional[Image.Image]:
    """Open image file safely as RGB."""
    try:
        img = Image.open(file_path)
        img.load()
        return img.convert("RGB")
    except Exception as e:
        logger.warning("open image failed: %s", e)
        return None


def _preprocess_image(
    img: Image.Image,
    *,
    max_side: int = 2200,
    to_grayscale: bool = False,
) -> Image.Image:
    """
    Preprocess for OCR:
    - convert RGB
    - optional grayscale
    - downscale if too large (memory safe)
    """
    if img.mode != "RGB":
        img = img.convert("RGB")

    w, h = img.size
    m = max(w, h)
    if m > max_side and max_side > 0:
        ratio = max_side / float(m)
        nw = max(1, int(w * ratio))
        nh = max(1, int(h * ratio))
        try:
            img = img.resize((nw, nh))
        except Exception:
            # fallback safest
            img = img.resize((nw, nh), resample=Image.BILINEAR)

    if to_grayscale:
        try:
            img = img.convert("L").convert("RGB")
        except Exception:
            pass

    return img


# ============================================================
# PDF helpers
# ============================================================
def _pdf_has_text_fast(pdf_path: str, min_chars: int = 120, max_pages: int = 3) -> Tuple[bool, str]:
    """
    ถ้า PDF มี text layer (ไม่ใช่สแกน) -> ดึง text ได้เลย (เร็ว/ฟรี)
    - ใช้ PyMuPDF (fitz)
    """
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(pdf_path)
        texts: List[str] = []
        n = min(doc.page_count, max(1, max_pages))
        for i in range(n):
            try:
                t = doc.load_page(i).get_text("text") or ""
            except Exception:
                t = ""
            t = (t or "").strip()
            if t:
                texts.append(t)

        joined = "\n\n".join(texts).strip()
        return (len(joined) >= min_chars, joined)

    except ModuleNotFoundError:
        # ไม่บังคับติดตั้ง pymupdf
        logger.info("PyMuPDF not installed; fast PDF text extraction unavailable.")
        return (False, "")

    except Exception as e:
        logger.warning("pdf_has_text_fast failed: %s", e)
        return (False, "")


def _render_pdf_to_images(pdf_path: str, max_pages: int = 20, zoom: float = 2.0) -> List[Image.Image]:
    """
    Render PDF -> PIL images ด้วย PyMuPDF (ไม่ต้อง poppler)
    """
    try:
        import fitz  # PyMuPDF
    except ModuleNotFoundError as e:
        raise RuntimeError("Missing dependency: PyMuPDF. Install with: pip install pymupdf") from e

    doc = fitz.open(pdf_path)
    imgs: List[Image.Image] = []

    n = min(doc.page_count, max(1, max_pages))
    mat = fitz.Matrix(zoom, zoom)

    for i in range(n):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        imgs.append(img)

    return imgs


# ============================================================
# Result meta (useful for debugging / UI)
# ============================================================
@dataclass
class OCRResult:
    text: str
    method: str  # "pdf_text" | "ocr_paddle_pdf" | "ocr_paddle_img" | "none"
    pages: int = 0
    warnings: List[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text or "",
            "method": self.method,
            "pages": int(self.pages or 0),
            "warnings": self.warnings or [],
        }


# ============================================================
# OCR Service
# ============================================================
class OCRService:
    """
    OCR pipeline (ฉลาด + deploy-friendly + คุม dependency):

    1) PDF (Text layer) -> extract ด้วย PyMuPDF เสมอ (เร็ว/ฟรี) แม้ปิด OCR
    2) PDF (Scan) / Image -> OCR เฉพาะเมื่อ ENABLE_OCR=1 และ OCR_PROVIDER พร้อม
       - OCR_PROVIDER: "paddle" | "document_ai" | "none"
    """

    def __init__(self):
        # เปิด OCR เฉพาะภาพ/สแกน (ค่า default = ปิด เพื่อ deploy ง่าย)
        self.enable_ocr: bool = _env_bool("ENABLE_OCR", default=False)

        # provider default none (กัน Render/Deploy ล้มจาก dependency หนัก)
        self.provider: str = _safe_str("OCR_PROVIDER", "none").lower()

        # PDF text-layer detection
        self.min_chars: int = _safe_int("OCR_MIN_TEXT_CHARS", 120)
        self.fast_check_pages: int = _safe_int("OCR_FAST_CHECK_PAGES", 3)

        # PDF render settings for OCR (scan pdf)
        self.max_pages: int = _safe_int("OCR_MAX_PAGES", 20)
        self.zoom: float = _safe_float("OCR_PDF_ZOOM", 2.0)

        # image preprocess
        self.max_side: int = _safe_int("OCR_IMG_MAX_SIDE", 2200)
        self.gray: bool = _env_bool("OCR_IMG_GRAYSCALE", default=False)

        # PaddleOCR options
        self.paddle_lang: str = _safe_str("PADDLE_OCR_LANG", "en")

        self._paddle = None
        self._paddle_ready = False

        # init lazily (only when needed)
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
            from paddleocr import PaddleOCR  # type: ignore

            self._paddle = PaddleOCR(
                use_angle_cls=True,
                lang=self.paddle_lang,
                show_log=False,
            )
            self._paddle_ready = True
            logger.info("PaddleOCR initialized (lang=%s)", self.paddle_lang)
        except ModuleNotFoundError:
            logger.warning("paddleocr not installed. OCR for images/scanned PDFs disabled.")
            self._paddle_ready = False
        except Exception as e:
            logger.warning("Failed to init PaddleOCR: %s", e)
            self._paddle_ready = False

    def _ensure_provider_ready(self) -> None:
        if not self.enable_ocr:
            return
        if self.provider == "paddle":
            self._init_paddle()

    # -------------------------
    # Public API
    # -------------------------
    def extract_text(self, file_path: str) -> str:
        """Compatibility: return text only (no meta)."""
        return self.extract_text_with_meta(file_path).text

    def extract_text_with_meta(self, file_path: str) -> OCRResult:
        """
        ฟังก์ชันหลัก (แนะนำ):
        - PDF: พยายามดึง text layer ก่อนเสมอ
        - ถ้าเป็นสแกน/รูป: OCR เฉพาะเมื่อ enable_ocr และ provider พร้อม
        """
        if not file_path:
            return OCRResult(text="", method="none", pages=0, warnings=["empty_path"])

        # 1) PDF: always try fast text extraction (works even when OCR disabled)
        if _is_pdf(file_path):
            has_text, text = _pdf_has_text_fast(
                file_path,
                min_chars=self.min_chars,
                max_pages=self.fast_check_pages,
            )
            if has_text:
                return OCRResult(text=text, method="pdf_text", pages=min(self.fast_check_pages, 3), warnings=[])

            # PDF scanned -> OCR (if enabled)
            return self._ocr_scanned_pdf(file_path)

        # 2) Image: OCR (if enabled)
        if _is_image(file_path):
            return self._ocr_image(file_path)

        # 3) Unknown type
        logger.warning("Unsupported file type for OCR/text extraction: %s", file_path)
        return OCRResult(text="", method="none", pages=0, warnings=["unsupported_file_type"])

    # -------------------------
    # OCR implementations
    # -------------------------
    def _ocr_scanned_pdf(self, pdf_path: str) -> OCRResult:
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return OCRResult(text="", method="none", pages=0, warnings=["ocr_disabled"])

        if self.provider == "document_ai":
            text = self._ocr_document_ai(pdf_path)
            return OCRResult(text=text, method="document_ai", pages=0, warnings=[])

        # default: paddle
        self._ensure_provider_ready()
        if not self._paddle_ready:
            return OCRResult(text="", method="none", pages=0, warnings=["paddle_not_ready"])

        try:
            pages = _render_pdf_to_images(pdf_path, max_pages=self.max_pages, zoom=self.zoom)
        except Exception as e:
            logger.warning("render_pdf_to_images failed: %s", e)
            return OCRResult(text="", method="none", pages=0, warnings=["pdf_render_failed"])

        # preprocess (memory safe)
        pre = [_preprocess_image(p, max_side=self.max_side, to_grayscale=self.gray) for p in pages]
        text = self._ocr_images_with_paddle(pre)
        return OCRResult(text=text, method="ocr_paddle_pdf", pages=len(pre), warnings=[])

    def _ocr_image(self, file_path: str) -> OCRResult:
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return OCRResult(text="", method="none", pages=0, warnings=["ocr_disabled"])

        if self.provider == "document_ai":
            text = self._ocr_document_ai(file_path)
            return OCRResult(text=text, method="document_ai", pages=0, warnings=[])

        # default: paddle
        self._ensure_provider_ready()
        if not self._paddle_ready:
            return OCRResult(text="", method="none", pages=0, warnings=["paddle_not_ready"])

        img = _open_image_safe(file_path)
        if img is None:
            return OCRResult(text="", method="none", pages=0, warnings=["image_open_failed"])

        img = _preprocess_image(img, max_side=self.max_side, to_grayscale=self.gray)
        text = self._ocr_images_with_paddle([img])
        return OCRResult(text=text, method="ocr_paddle_img", pages=1, warnings=[])

    def _ocr_images_with_paddle(self, images: List[Image.Image]) -> str:
        """
        Run PaddleOCR on list of PIL images.
        - รวม text ตามลำดับ
        - แทรกตัวแบ่งหน้า (ถ้าหลายหน้า) เพื่อให้ extractor เดาง่ายขึ้น
        """
        if not self._paddle_ready or not self._paddle:
            return ""

        try:
            import numpy as np  # type: ignore
        except ModuleNotFoundError:
            logger.warning("numpy not installed. Cannot run PaddleOCR.")
            return ""

        pages_out: List[str] = []

        for page_idx, img in enumerate(images, start=1):
            chunks: List[str] = []
            try:
                arr = np.array(img)
                result = self._paddle.ocr(arr, cls=True) or []
            except Exception as e:
                logger.warning("PaddleOCR failed on page %s: %s", page_idx, e)
                continue

            # result format (common): [ [ [box], (text, score) ], ... ] per line
            # บางเวอร์ชัน: result = [ [ [box, (text, score)], ... ], ... ]
            try:
                for line in result:
                    for item in line:
                        # item usually: [box, (text, score)]
                        try:
                            text_score = item[1]
                            if isinstance(text_score, (list, tuple)) and len(text_score) >= 1:
                                txt = text_score[0]
                                if txt:
                                    chunks.append(str(txt))
                        except Exception:
                            continue
            except Exception:
                # fallback: stringify everything
                try:
                    for x in (result or []):
                        chunks.append(str(x))
                except Exception:
                    pass

            page_text = "\n".join(chunks).strip()
            if page_text:
                pages_out.append(page_text)

        if not pages_out:
            return ""

        if len(pages_out) == 1:
            return pages_out[0].strip()

        # multi-page: add clear separators
        joined = []
        for i, ptxt in enumerate(pages_out, start=1):
            joined.append(f"\n\n===== PAGE {i} =====\n\n{ptxt}")
        return "".join(joined).strip()

    def _ocr_document_ai(self, file_path: str) -> str:
        """
        Placeholder for Google Document AI / other OCR providers.
        คุณสามารถต่อเพิ่มทีหลังได้ (แนะนำถ้าอยาก OCR ไทยแม่น + ตาราง)
        """
        raise NotImplementedError("OCR_PROVIDER=document_ai not implemented in this build.")


# ============================================================
# Compatibility helper (kept name)
# ============================================================
def maybe_ocr_to_text(file_path: str) -> str:
    """
    backward-compatible helper for job_worker.py

    ✅ behavior (ตามที่ต้องการ):
    - PDF มี text layer: extract ได้เสมอ (แม้ปิด OCR)
    - PDF สแกน/รูป: OCR เฉพาะเมื่อเปิด ENABLE_OCR=1 และ OCR_PROVIDER=paddle/document_ai
    """
    try:
        return OCRService().extract_text(file_path)
    except Exception as e:
        logger.warning("maybe_ocr_to_text failed: %s", e)
        return ""
