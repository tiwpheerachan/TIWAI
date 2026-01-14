# -*- coding: utf-8 -*-
"""
ai_document_router.py (FIXED VERSION)

Improvements:
1. ✅ Hybrid routing: Try rule-based extractors first, then AI fallback
2. ✅ Thai prompts support
3. ✅ Better error handling
4. ✅ Graceful degradation when AI is disabled
5. ✅ Integration with existing classifier

Goal:
- For multi-page PDFs (especially META/GOOGLE), create a routing plan per segment
- Each segment is then sent to your existing ai_fill_peak_row() to produce PEAK row patch
- BUT: Try rule-based extractors first for known formats (fast + free)

Important:
- This file is placed under app/extractors/, so imports to services must use ..services.*
- Prompts are loaded from ./prompts folder.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .multi_page_analyzer import Analysis, Segment, analyze_pdf_bytes, analyze_text_as_single_segment

# ✅ Import Thai prompts support
from .prompts import (
    ROUTER_SYSTEM,
    ROUTER_USER,
    META_ADS_USER,
    GOOGLE_ADS_USER,
    GENERIC_DOC_USER,
    META_ADS_STATEMENT_TH,
    UNKNOWN_GENERAL_TH,
    get_prompt_for_route,
)

# ✅ Import rule-based extractors for hybrid approach
try:
    from .ads_meta import extract_meta_ads
    from .ads_google import extract_google_ads
    _RULE_BASED_OK = True
except ImportError:
    extract_meta_ads = None
    extract_google_ads = None
    _RULE_BASED_OK = False

# Use your existing LLM filler (already guarded by env ENABLE_LLM)
try:
    from ..services.ai_service import ai_fill_peak_row
    _AI_SERVICE_OK = True
except ImportError:
    ai_fill_peak_row = None
    _AI_SERVICE_OK = False

# ✅ Import classifier for better routing
try:
    from ..services.classifier import classify_platform
    _CLASSIFIER_OK = True
except ImportError:
    classify_platform = None
    _CLASSIFIER_OK = False


logger = logging.getLogger(__name__)


@dataclass
class RoutedJob:
    route_name: str               # e.g. "meta_ads", "google_ads", "generic"
    prompt_name: str              # prompt file label
    platform_hint: str            # "META"|"GOOGLE"|...
    segment_index: int
    page_indices: List[int]
    merged_text: str
    partial_row: Dict[str, Any]
    meta: Dict[str, Any]
    use_rule_based: bool = False  # ✅ NEW: Flag to use rule-based extractor


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _choose_route_from_segment(
    seg: Segment,
    filename: str = "",
    use_classifier: bool = True
) -> Tuple[str, str, str, bool]:
    """
    ✅ IMPROVED: Hybrid routing with classifier + rule-based priority
    
    Returns: (route_name, prompt_name, platform_hint, use_rule_based)
    """
    
    # ============================================================
    # STEP 1: Try classifier first (if available)
    # ============================================================
    detected_platform = "unknown"
    
    if use_classifier and _CLASSIFIER_OK and classify_platform:
        try:
            detected_platform = classify_platform(
                seg.merged_text,
                filename=filename
            ).lower()
        except Exception as e:
            logger.warning(f"Classifier failed: {e}")
            detected_platform = "unknown"
    
    # ============================================================
    # STEP 2: Fallback to analyzer profile
    # ============================================================
    if detected_platform == "unknown":
        ph = (seg.seg_profile.platform_hint or "UNKNOWN").upper()
        kind = (seg.seg_profile.doc_kind or "GENERIC").upper()
        
        if ph == "META" or kind.startswith("META"):
            detected_platform = "ads_meta"
        elif ph == "GOOGLE" or kind.startswith("GOOGLE"):
            detected_platform = "ads_google"
        else:
            detected_platform = "unknown"
    
    # ============================================================
    # STEP 3: Route based on detected platform
    # ============================================================
    
    # META Ads - Use rule-based if available
    if detected_platform in ["ads_meta", "meta", "meta_ads"]:
        use_rule = _RULE_BASED_OK and extract_meta_ads is not None
        return ("meta_ads", "meta_ads_user", "META", use_rule)
    
    # Google Ads - Use rule-based if available
    if detected_platform in ["ads_google", "google", "google_ads"]:
        use_rule = _RULE_BASED_OK and extract_google_ads is not None
        return ("google_ads", "google_ads_user", "GOOGLE", use_rule)
    
    # Marketplace - These should be handled by main extract_service
    # So we mark as "not rule-based here" to trigger AI fallback
    if detected_platform in ["shopee", "lazada", "tiktok", "spx"]:
        return ("marketplace", "generic_doc_user", detected_platform.upper(), False)
    
    # Unknown - AI fallback
    return ("generic", "generic_doc_user", "UNKNOWN", False)


def _build_partial_row_for_ai(
    route_name: str,
    platform_hint: str,
    cfg: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    We only set safe defaults. Your job_worker/extract_service will still produce base_row;
    This module is mainly for "unknown docs" OR "multi-page meta/google docs".
    """
    cfg = cfg or {}
    partial: Dict[str, Any] = {}

    # allow UI to pass shop_name / username as cfg["shop_name"]
    shop_name = str(cfg.get("shop_name") or "").strip()
    if shop_name:
        partial["_shop_name"] = shop_name

    # group hint (optional)
    if platform_hint in {"META", "GOOGLE"}:
        partial["U_group"] = "Advertising Expense"
    
    # ✅ Add platform hint for AI
    partial["_platform_hint"] = platform_hint
    
    return partial


def build_routing_plan_from_pdf(
    pdf_bytes: bytes,
    filename: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[RoutedJob]]:
    """
    ✅ IMPROVED: Hybrid routing plan
    
    1) Analyze pages -> segments
    2) Route each segment with classifier + analyzer
    3) Mark rule-based vs AI
    4) Prepare RoutedJob list
    """
    cfg = cfg or {}
    
    # ✅ Add error handling
    try:
        max_pages = int(os.getenv("ROUTER_MAX_PAGES", "60") or "60")
        analysis: Analysis = analyze_pdf_bytes(
            pdf_bytes,
            filename=filename,
            max_pages=max_pages
        )
    except Exception as e:
        logger.error(f"PDF analysis failed: {e}")
        # Return empty analysis
        return {
            "filename": filename,
            "total_pages": 0,
            "error": str(e)[:200]
        }, []

    jobs: List[RoutedJob] = []
    
    for seg in analysis.segments:
        # ✅ Use improved routing
        route_name, prompt_name, platform_hint, use_rule = _choose_route_from_segment(
            seg,
            filename=filename,
            use_classifier=True
        )
        
        partial = _build_partial_row_for_ai(route_name, platform_hint, cfg=cfg)

        meta = {
            "route_name": route_name,
            "prompt_name": prompt_name,
            "platform_hint": platform_hint,
            "segment_index": seg.segment_index,
            "page_indices": seg.page_indices,
            "segment_profile": seg.seg_profile.to_meta(),
            "use_rule_based": use_rule,  # ✅ NEW
        }

        jobs.append(
            RoutedJob(
                route_name=route_name,
                prompt_name=prompt_name,
                platform_hint=platform_hint,
                segment_index=seg.segment_index,
                page_indices=seg.page_indices,
                merged_text=seg.merged_text,
                partial_row=partial,
                meta=meta,
                use_rule_based=use_rule,  # ✅ NEW
            )
        )

    return analysis.to_meta(), jobs


def build_routing_plan_from_text(
    text: str,
    filename: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[RoutedJob]]:
    """
    ✅ IMPROVED: Hybrid routing plan for text
    """
    cfg = cfg or {}
    
    # ✅ Add error handling
    try:
        analysis = analyze_text_as_single_segment(text or "", filename=filename)
    except Exception as e:
        logger.error(f"Text analysis failed: {e}")
        return {
            "filename": filename,
            "total_pages": 1,
            "error": str(e)[:200]
        }, []
    
    jobs: List[RoutedJob] = []
    
    for seg in analysis.segments:
        # ✅ Use improved routing
        route_name, prompt_name, platform_hint, use_rule = _choose_route_from_segment(
            seg,
            filename=filename,
            use_classifier=True
        )
        
        partial = _build_partial_row_for_ai(route_name, platform_hint, cfg=cfg)
        
        meta = {
            "route_name": route_name,
            "prompt_name": prompt_name,
            "platform_hint": platform_hint,
            "segment_index": seg.segment_index,
            "page_indices": seg.page_indices,
            "segment_profile": seg.seg_profile.to_meta(),
            "use_rule_based": use_rule,  # ✅ NEW
        }
        
        jobs.append(
            RoutedJob(
                route_name=route_name,
                prompt_name=prompt_name,
                platform_hint=platform_hint,
                segment_index=seg.segment_index,
                page_indices=seg.page_indices,
                merged_text=seg.merged_text,
                partial_row=partial,
                meta=meta,
                use_rule_based=use_rule,  # ✅ NEW
            )
        )
    
    return analysis.to_meta(), jobs


def run_ai_jobs_to_peak_rows(
    jobs: List[RoutedJob],
    source_filename: str,
    lang: str = "en",  # ✅ NEW: "en" or "th"
) -> List[Dict[str, Any]]:
    """
    ✅ IMPROVED: Execute with hybrid approach
    
    1) Try rule-based extractor if marked
    2) Fallback to AI if rule-based fails or not available
    3) Return PEAK patch dicts
    
    Note: ai_fill_peak_row already has env guards:
      ENABLE_LLM=1 and OPENAI_API_KEY set.
    """
    out: List[Dict[str, Any]] = []
    
    for job in jobs:
        row = None
        extraction_method = "unknown"
        error_msg = None
        
        # ============================================================
        # STEP 1: Try rule-based extractor first (if marked)
        # ============================================================
        if job.use_rule_based:
            try:
                if job.route_name == "meta_ads" and extract_meta_ads:
                    row = extract_meta_ads(
                        job.merged_text,
                        filename=source_filename
                    )
                    extraction_method = "rule_based_meta"
                    logger.info(f"Segment {job.segment_index}: Used rule-based Meta extractor")
                
                elif job.route_name == "google_ads" and extract_google_ads:
                    row = extract_google_ads(
                        job.merged_text,
                        filename=source_filename
                    )
                    extraction_method = "rule_based_google"
                    logger.info(f"Segment {job.segment_index}: Used rule-based Google extractor")
                
            except Exception as e:
                logger.warning(f"Rule-based extraction failed for segment {job.segment_index}: {e}")
                error_msg = f"Rule-based failed: {str(e)[:100]}"
                row = None
        
        # ============================================================
        # STEP 2: AI fallback (if rule-based failed or not used)
        # ============================================================
        if row is None:
            # Check if AI is available
            if not _AI_SERVICE_OK or ai_fill_peak_row is None:
                # No AI available → return error row
                out.append({
                    "_route_name": job.route_name,
                    "_prompt_name": job.prompt_name,
                    "_segment_index": job.segment_index,
                    "_page_indices": ",".join(str(i) for i in job.page_indices),
                    "_extraction_method": "failed",
                    "_error": "AI service not available and rule-based failed",
                })
                continue
            
            # Check if AI is enabled
            if not _env_bool("ENABLE_AI_EXTRACT", False):
                out.append({
                    "_route_name": job.route_name,
                    "_prompt_name": job.prompt_name,
                    "_segment_index": job.segment_index,
                    "_page_indices": ",".join(str(i) for i in job.page_indices),
                    "_extraction_method": "skipped",
                    "_error": "AI extraction disabled (ENABLE_AI_EXTRACT=0)",
                })
                continue
            
            # ✅ Use AI with proper prompt (English or Thai)
            try:
                # Get appropriate prompt based on language
                custom_prompt = get_prompt_for_route(job.route_name, lang=lang)
                
                ai_row = ai_fill_peak_row(
                    text=job.merged_text,
                    platform_hint=job.platform_hint,
                    partial_row=job.partial_row,
                    source_filename=source_filename,
                    custom_prompt=custom_prompt if custom_prompt else None,
                )
                
                if isinstance(ai_row, dict):
                    row = ai_row
                    extraction_method = f"ai_{lang}"
                    logger.info(f"Segment {job.segment_index}: Used AI extractor ({lang})")
                else:
                    error_msg = "ai_fill_peak_row returned non-dict"
            
            except Exception as e:
                logger.error(f"AI extraction failed for segment {job.segment_index}: {e}")
                error_msg = f"AI failed: {str(e)[:100]}"
        
        # ============================================================
        # STEP 3: Prepare output row
        # ============================================================
        if row and isinstance(row, dict):
            # Success! Attach metadata
            row["_route_name"] = job.route_name
            row["_prompt_name"] = job.prompt_name
            row["_segment_index"] = job.segment_index
            row["_page_indices"] = ",".join(str(i) for i in job.page_indices)
            row["_extraction_method"] = extraction_method
            
            if error_msg:
                row["_extraction_notes"] = error_msg
            
            out.append(row)
        else:
            # Failed! Return error row
            out.append({
                "_route_name": job.route_name,
                "_prompt_name": job.prompt_name,
                "_segment_index": job.segment_index,
                "_page_indices": ",".join(str(i) for i in job.page_indices),
                "_extraction_method": "failed",
                "_error": error_msg or "Extraction returned no data",
            })
    
    return out


# ============================================================
# ✅ NEW: Convenience function for single-doc extraction
# ============================================================

def extract_with_router(
    text: str = "",
    pdf_bytes: Optional[bytes] = None,
    filename: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    lang: str = "en",
) -> List[Dict[str, Any]]:
    """
    Convenience function: Analyze + Route + Extract in one call
    
    Args:
        text: Text content (if not PDF)
        pdf_bytes: PDF bytes (if PDF)
        filename: Original filename
        cfg: Config dict
        lang: "en" or "th" for prompts
    
    Returns:
        List of PEAK row dicts (one per segment)
    """
    
    cfg = cfg or {}
    
    # Build routing plan
    if pdf_bytes:
        analysis_meta, jobs = build_routing_plan_from_pdf(
            pdf_bytes=pdf_bytes,
            filename=filename,
            cfg=cfg
        )
    else:
        analysis_meta, jobs = build_routing_plan_from_text(
            text=text,
            filename=filename,
            cfg=cfg
        )
    
    # Execute jobs
    rows = run_ai_jobs_to_peak_rows(
        jobs=jobs,
        source_filename=filename,
        lang=lang
    )
    
    return rows


__all__ = [
    "build_routing_plan_from_pdf",
    "build_routing_plan_from_text",
    "run_ai_jobs_to_peak_rows",
    "extract_with_router",  # ✅ NEW
]