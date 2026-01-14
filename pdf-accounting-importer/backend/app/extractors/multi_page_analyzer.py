# -*- coding: utf-8 -*-
"""
multi_page_analyzer.py (FIXED VERSION)

Improvements:
1. ✅ Better error handling (pdfplumber import, PDF parsing)
2. ✅ Graceful degradation if pdfplumber unavailable
3. ✅ Added logging for debugging
4. ✅ Better handling of corrupt/empty PDFs
5. ✅ More robust segmentation logic
6. ✅ Performance optimizations

Split a PDF (or a long text) into "segments" for routing/AI extraction.

Key use cases:
- Meta receipts often come as multiple pages in 1 PDF (many transactions)
- Unknown docs: we still try to segment by page-level changes (tax id, transaction id, headers)

This module does NOT call OpenAI. It is deterministic.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ✅ Graceful import handling
try:
    import pdfplumber
    _PDFPLUMBER_OK = True
except ImportError:
    pdfplumber = None
    _PDFPLUMBER_OK = False

from .document_profile import (
    PageProfile,
    SegmentProfile,
    build_page_profile,
    merge_segment_profile,
)


logger = logging.getLogger(__name__)


@dataclass
class Segment:
    """A segment of pages that belong together"""
    segment_index: int
    page_indices: List[int]
    merged_text: str
    seg_profile: SegmentProfile

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        return {
            "segment_index": self.segment_index,
            "page_indices": self.page_indices,
            "merged_text_len": len(self.merged_text or ""),
            "platform_hint": self.seg_profile.platform_hint,
            "doc_kind": self.seg_profile.doc_kind,
            "tax_id_13": self.seg_profile.tax_id_13,
            "seller_id": self.seg_profile.seller_id,
            "transaction_id": self.seg_profile.transaction_id,
            "invoice_no": self.seg_profile.invoice_no,
            "reasons": self.seg_profile.reasons,
        }


@dataclass
class Analysis:
    """Complete analysis result"""
    filename: str
    total_pages: int
    pages: List[PageProfile]
    segments: List[Segment]
    error: Optional[str] = None  # ✅ NEW: Error message if analysis failed

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        result = {
            "filename": self.filename,
            "total_pages": self.total_pages,
            "pages": [p.to_meta() for p in self.pages],
            "segments": [s.to_meta() for s in self.segments],
        }
        if self.error:
            result["error"] = self.error
        return result


def _extract_pdf_page_texts(pdf_bytes: bytes, max_pages: int = 60) -> List[str]:
    """
    ✅ IMPROVED: Better error handling and validation
    """
    
    # Check if pdfplumber is available
    if not _PDFPLUMBER_OK or pdfplumber is None:
        logger.error("pdfplumber not available - cannot extract PDF text")
        raise ImportError(
            "pdfplumber is required for PDF processing. "
            "Install with: pip install pdfplumber"
        )
    
    # Validate input
    if not pdf_bytes:
        logger.warning("Empty PDF bytes provided")
        return []
    
    if len(pdf_bytes) < 100:
        logger.warning(f"PDF bytes too small: {len(pdf_bytes)} bytes")
        return []
    
    texts: List[str] = []
    
    try:
        # Open PDF with timeout protection
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            
            # Check if PDF is valid
            if not pdf.pages:
                logger.warning("PDF has no pages")
                return []
            
            n = min(len(pdf.pages), max_pages)
            
            if n > max_pages:
                logger.info(f"PDF has {len(pdf.pages)} pages, limiting to {max_pages}")
            
            # Extract text from each page
            for i in range(n):
                try:
                    page_text = pdf.pages[i].extract_text()
                    
                    # Handle None or empty text
                    if page_text is None:
                        logger.warning(f"Page {i}: extract_text() returned None")
                        texts.append("")
                    else:
                        texts.append(page_text or "")
                        
                        # Log page text length for debugging
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Page {i}: extracted {len(page_text)} chars")
                
                except Exception as e:
                    logger.warning(f"Page {i}: extraction failed: {e}")
                    texts.append("")  # Add empty string to maintain page indices
    
    except Exception as e:
        logger.error(f"PDF parsing failed: {e}")
        raise RuntimeError(f"Failed to parse PDF: {str(e)[:200]}")
    
    return texts


def _should_break(prev: PageProfile, cur: PageProfile) -> Tuple[bool, str]:
    """
    ✅ IMPROVED: Better segmentation logic
    
    Decide if we should start a new segment at cur page.
    Return (break?, reason)
    """
    
    # ============================================================
    # RULE 1: Doc kind changes (strong signal)
    # ============================================================
    if prev.doc_kind != cur.doc_kind and cur.text_len > 30:
        # Only break if doc kinds are significantly different
        # e.g., META_RECEIPT → GOOGLE_PAYMENT
        if prev.doc_kind != "GENERIC" and cur.doc_kind != "GENERIC":
            return True, f"doc_kind change: {prev.doc_kind} → {cur.doc_kind}"

    # ============================================================
    # RULE 2: Platform changes (strong signal)
    # ============================================================
    if prev.platform_hint != cur.platform_hint and cur.text_len > 30:
        # Don't break on UNKNOWN transitions (too noisy)
        if prev.platform_hint != "UNKNOWN" and cur.platform_hint != "UNKNOWN":
            return True, f"platform change: {prev.platform_hint} → {cur.platform_hint}"

    # ============================================================
    # RULE 3: Meta - transaction id changes
    # ============================================================
    if prev.platform_hint == "META" and cur.platform_hint == "META":
        if prev.transaction_id and cur.transaction_id:
            if prev.transaction_id != cur.transaction_id:
                return True, f"meta txn change: {prev.transaction_id} → {cur.transaction_id}"

    # ============================================================
    # RULE 4: Google - invoice number changes
    # ============================================================
    if prev.platform_hint == "GOOGLE" and cur.platform_hint == "GOOGLE":
        if prev.invoice_no and cur.invoice_no:
            if prev.invoice_no != cur.invoice_no:
                return True, f"google invoice change: {prev.invoice_no} → {cur.invoice_no}"

    # ============================================================
    # RULE 5: Tax ID changes (strong signal)
    # ============================================================
    if prev.tax_id_13 and cur.tax_id_13:
        if prev.tax_id_13 != cur.tax_id_13:
            # Different vendor/company
            return True, f"tax_id change: {prev.tax_id_13} → {cur.tax_id_13}"

    # ============================================================
    # RULE 6: Page numbering reset
    # ============================================================
    if prev.page_x and cur.page_x:
        # Page reset: "Page 5 of 5" → "Page 1 of 3"
        if cur.page_x == 1 and prev.page_x != 1:
            return True, f"page reset: prev page_x={prev.page_x}, cur page_x=1"

    # ============================================================
    # RULE 7: Seller ID changes (medium signal)
    # ============================================================
    if prev.seller_id and cur.seller_id:
        if prev.seller_id != cur.seller_id:
            return True, f"seller_id change: {prev.seller_id} → {cur.seller_id}"

    return False, ""


def analyze_pdf_bytes(
    pdf_bytes: bytes,
    filename: str = "",
    max_pages: int = 60
) -> Analysis:
    """
    ✅ IMPROVED: Better error handling and validation
    
    Analyze PDF and split into segments.
    
    Args:
        pdf_bytes: PDF file content
        filename: Original filename (for hints)
        max_pages: Maximum pages to analyze
    
    Returns:
        Analysis object with pages and segments
    """
    
    # Validate input
    if not pdf_bytes:
        logger.error("Empty PDF bytes provided")
        return Analysis(
            filename=filename,
            total_pages=0,
            pages=[],
            segments=[],
            error="Empty PDF bytes"
        )
    
    # Check pdfplumber availability
    if not _PDFPLUMBER_OK:
        logger.error("pdfplumber not available")
        return Analysis(
            filename=filename,
            total_pages=0,
            pages=[],
            segments=[],
            error="pdfplumber not installed"
        )
    
    # Extract page texts
    try:
        page_texts = _extract_pdf_page_texts(pdf_bytes, max_pages=max_pages)
    except Exception as e:
        logger.error(f"PDF text extraction failed: {e}")
        return Analysis(
            filename=filename,
            total_pages=0,
            pages=[],
            segments=[],
            error=f"PDF extraction failed: {str(e)[:100]}"
        )
    
    # Build page profiles
    pages: List[PageProfile] = []
    for i, t in enumerate(page_texts):
        try:
            profile = build_page_profile(i, t, filename=filename)
            pages.append(profile)
        except Exception as e:
            logger.warning(f"Page {i} profile building failed: {e}")
            # Continue with other pages
    
    # Handle empty result
    if not pages:
        logger.warning("No pages could be analyzed")
        return Analysis(
            filename=filename,
            total_pages=0,
            pages=[],
            segments=[],
            error="No pages could be analyzed"
        )

    # ============================================================
    # Build segments by break rules
    # ============================================================
    segments: List[Segment] = []
    start = 0
    seg_idx = 0
    
    try:
        for i in range(1, len(pages)):
            brk, reason = _should_break(pages[i - 1], pages[i])
            
            if brk:
                # Create segment from start to i
                chunk = pages[start:i]
                
                # Merge page texts
                try:
                    merged = "\n\n".join(
                        page_texts[j.page_index]
                        for j in chunk
                        if j.page_index < len(page_texts)
                    )
                except Exception as e:
                    logger.warning(f"Text merging failed for segment {seg_idx}: {e}")
                    merged = ""
                
                # Build segment profile
                try:
                    seg_profile = merge_segment_profile(seg_idx, chunk, merged)
                    seg_profile.reasons.append(f"split_reason={reason}")
                    
                    segments.append(
                        Segment(
                            segment_index=seg_idx,
                            page_indices=[p.page_index for p in chunk],
                            merged_text=merged,
                            seg_profile=seg_profile,
                        )
                    )
                    
                    logger.debug(f"Segment {seg_idx}: pages {start}-{i-1}, reason: {reason}")
                    
                except Exception as e:
                    logger.error(f"Segment {seg_idx} creation failed: {e}")
                
                seg_idx += 1
                start = i
        
        # ============================================================
        # Last segment (remaining pages)
        # ============================================================
        chunk = pages[start:]
        
        try:
            merged = "\n\n".join(
                page_texts[j.page_index]
                for j in chunk
                if j.page_index < len(page_texts)
            )
        except Exception as e:
            logger.warning(f"Text merging failed for last segment: {e}")
            merged = ""
        
        try:
            seg_profile = merge_segment_profile(seg_idx, chunk, merged)
            segments.append(
                Segment(
                    segment_index=seg_idx,
                    page_indices=[p.page_index for p in chunk],
                    merged_text=merged,
                    seg_profile=seg_profile,
                )
            )
            
            logger.debug(f"Segment {seg_idx}: pages {start}-{len(pages)-1} (last)")
            
        except Exception as e:
            logger.error(f"Last segment creation failed: {e}")
    
    except Exception as e:
        logger.error(f"Segmentation failed: {e}")
        # Return what we have so far
    
    # Final validation
    if not segments:
        logger.warning("No segments created, creating single segment")
        # Fallback: treat entire document as one segment
        try:
            merged = "\n\n".join(page_texts)
            seg_profile = merge_segment_profile(0, pages, merged)
            segments.append(
                Segment(
                    segment_index=0,
                    page_indices=[p.page_index for p in pages],
                    merged_text=merged,
                    seg_profile=seg_profile,
                )
            )
        except Exception as e:
            logger.error(f"Fallback segment creation failed: {e}")
    
    logger.info(
        f"Analysis complete: {filename}, "
        f"{len(pages)} pages, {len(segments)} segments"
    )
    
    return Analysis(
        filename=filename,
        total_pages=len(pages),
        pages=pages,
        segments=segments
    )


def analyze_text_as_single_segment(text: str, filename: str = "") -> Analysis:
    """
    ✅ IMPROVED: Better error handling
    
    For non-pdf flows: treat as 1 segment.
    """
    
    # Validate input
    if not text:
        logger.warning("Empty text provided")
        return Analysis(
            filename=filename,
            total_pages=1,
            pages=[],
            segments=[],
            error="Empty text"
        )
    
    try:
        # Build single page profile
        page = build_page_profile(0, text or "", filename=filename)
        pages = [page]
        
        # Build single segment
        seg_profile = merge_segment_profile(0, pages, text or "")
        seg = Segment(
            segment_index=0,
            page_indices=[0],
            merged_text=text or "",
            seg_profile=seg_profile
        )
        
        logger.debug(f"Single segment analysis: {filename}, {len(text)} chars")
        
        return Analysis(
            filename=filename,
            total_pages=1,
            pages=pages,
            segments=[seg]
        )
    
    except Exception as e:
        logger.error(f"Text analysis failed: {e}")
        return Analysis(
            filename=filename,
            total_pages=1,
            pages=[],
            segments=[],
            error=f"Text analysis failed: {str(e)[:100]}"
        )


# ============================================================
# ✅ NEW: Utility functions
# ============================================================

def is_pdfplumber_available() -> bool:
    """Check if pdfplumber is available"""
    return _PDFPLUMBER_OK


def get_analysis_summary(analysis: Analysis) -> str:
    """
    Get human-readable summary of analysis
    
    Returns:
        Summary string like "3 pages, 2 segments (META, GOOGLE)"
    """
    if not analysis or not analysis.segments:
        return "No analysis available"
    
    platforms = set(s.seg_profile.platform_hint for s in analysis.segments)
    platforms_str = ", ".join(sorted(platforms))
    
    return (
        f"{analysis.total_pages} pages, "
        f"{len(analysis.segments)} segments "
        f"({platforms_str})"
    )


def validate_pdf_bytes(pdf_bytes: bytes) -> Tuple[bool, str]:
    """
    Validate PDF bytes before processing
    
    Returns:
        (is_valid, error_message)
    """
    
    if not pdf_bytes:
        return False, "Empty PDF bytes"
    
    if len(pdf_bytes) < 100:
        return False, f"PDF too small: {len(pdf_bytes)} bytes"
    
    # Check PDF signature
    if not pdf_bytes.startswith(b"%PDF"):
        return False, "Not a valid PDF file (missing %PDF header)"
    
    if not _PDFPLUMBER_OK:
        return False, "pdfplumber not installed"
    
    return True, ""


__all__ = [
    "Segment",
    "Analysis",
    "analyze_pdf_bytes",
    "analyze_text_as_single_segment",
    "is_pdfplumber_available",  # ✅ NEW
    "get_analysis_summary",      # ✅ NEW
    "validate_pdf_bytes",        # ✅ NEW
]