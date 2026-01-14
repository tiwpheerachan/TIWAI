# backend/app/services/classifier.py
"""
Platform Classifier - Enhanced Version for 8 Platforms

✅ Improvements:
1. ✅ Support for 8 platforms (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
2. ✅ Meta/Facebook Ads detection
3. ✅ Google Ads detection
4. ✅ Thai Tax Invoice detection
5. ✅ Priority-based classification (prevent misclassification)
6. ✅ Integration with export_service/ai_service constants
7. ✅ Better error handling and logging
8. ✅ Metadata tracking
9. ✅ Relaxed thresholds (from original)
10. ✅ Enhanced debug support
"""
from __future__ import annotations

import re
import logging
from typing import Literal, Dict, Tuple, Optional

from ..utils.text_utils import normalize_text

# Setup logger
logger = logging.getLogger(__name__)

# ✅ 8 platforms (aligned with export_service and ai_service)
PlatformLabel = Literal[
    "META",      # Meta/Facebook Ads
    "GOOGLE",    # Google Ads
    "SHOPEE",    # Shopee marketplace
    "LAZADA",    # Lazada marketplace
    "TIKTOK",    # TikTok Shop
    "SPX",       # Shopee Express
    "THAI_TAX",  # Thai Tax Invoice
    "UNKNOWN"    # Unknown/Other
]

# ---------------------------------------------------------------------
# Strong ID Regex (High Confidence)
# ---------------------------------------------------------------------

# ✅ Meta/Facebook Ads patterns
RE_META_RECEIPT = re.compile(
    r"\bRC\s*META\s*[A-Z0-9\-/]{6,}\b",
    re.IGNORECASE
)
RE_META_IRELAND = re.compile(
    r"meta\s*platforms?\s*ireland",
    re.IGNORECASE
)
RE_FACEBOOK = re.compile(
    r"\b(facebook|fb\s*ads|instagram\s*ads)\b",
    re.IGNORECASE
)

# ✅ Google Ads patterns
RE_GOOGLE_PAYMENT = re.compile(
    r"\b[VW]\s*\d{15,20}\b",  # Payment numbers like V0971174339667745
    re.IGNORECASE
)
RE_GOOGLE_ASIA = re.compile(
    r"google\s*asia\s*pacific",
    re.IGNORECASE
)
RE_GOOGLE_ADS = re.compile(
    r"\b(google\s*ad(?:s|words)?|google\s*advertising)\b",
    re.IGNORECASE
)

# ✅ Thai Tax Invoice patterns
RE_THAI_TAX_INVOICE = re.compile(
    r"(ใบกำกับภาษี|ใบเสร็จรับเงิน|tax\s*invoice)",
    re.IGNORECASE
)
RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")
RE_BRANCH_5 = re.compile(r"(?:branch|สาขา)\s*[:#]?\s*(\d{5})", re.IGNORECASE)

# SPX: RCSPX... (handle whitespace/newline)
RE_SPX_RCSPX = re.compile(r"\bRCS\s*PX\s*[A-Z0-9\-/]{6,}\b", re.IGNORECASE)
RE_SPX_RCS_ANY = re.compile(r"\bRCS\s*[A-Z0-9]{3,}\b", re.IGNORECASE)

# Lazada: THMPTIxxxxxxxxxxxxxxxx
RE_LAZADA_THMPTI = re.compile(r"\bTHMPTI\s*\d{10,20}\b", re.IGNORECASE)

# TikTok: TTSTH* or TikTok Shop
RE_TIKTOK_TTSTH = re.compile(r"\bTTSTH[0-9A-Z\-/]*\b", re.IGNORECASE)
RE_TIKTOK_WORD = re.compile(r"\btiktok\b", re.IGNORECASE)

# Shopee: TIV-/TIR- patterns (strong)
RE_SHOPEE_TIV = re.compile(r"\bTIV\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_TIR = re.compile(r"\bTIR\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_WORD = re.compile(r"\bshopee\b", re.IGNORECASE)

# Shopee TRS is weak (only count with Shopee context)
RE_SHOPEE_TRS = re.compile(r"\bTRS\b", re.IGNORECASE)

# ---------------------------------------------------------------------
# Signals (Soft Keywords)
# ---------------------------------------------------------------------

# ✅ Meta/Facebook Ads signals
META_SIGS_STRONG = (
    "meta platforms ireland",
    "facebook ireland",
    "rcmeta",
    "meta ads receipt",
    "facebook ads receipt",
    "instagram ads",
)
META_SIGS_WEAK = (
    "meta", "facebook", "fb ads", "meta ads",
    "reverse charge", "vat reverse",
)

# ✅ Google Ads signals
GOOGLE_SIGS_STRONG = (
    "google asia pacific",
    "google advertising",
    "google ads payment",
    "google adwords",
)
GOOGLE_SIGS_WEAK = (
    "google ads", "google payment", "adwords",
)

# ✅ Thai Tax Invoice signals
THAI_TAX_SIGS = (
    "ใบกำกับภาษี",
    "ใบเสร็จรับเงิน",
    "tax invoice",
    "เลขประจำตัวผู้เสียภาษี",
    "เลขทะเบียน",
    "สำนักงานใหญ่",
)

# Marketplace signals
SHOPEE_SIGS = (
    "shopee", "shopee-ti", "shopee-tiv", "shopee-tir",
    "tiv-", "tir-", "ช้อปปี้", "shopee (thailand)",
)
LAZADA_SIGS = (
    "lazada", "lazada invoice", "lzd", "laz", "ลาซาด้า",
)
TIKTOK_SIGS = (
    "tiktok", "tiktok shop", "tt shop", "tiktok commerce", "ติ๊กต็อก",
)
SPX_SIGS = (
    "spx", "spx express", "standard express", "rcs", "rcspx",
    "spx (thailand)", "spx express (thailand)", "shopee express",
)

# Generic ads signals (for old "ads" category, now mostly replaced by META/GOOGLE)
ADS_SIGS_STRONG = (
    "ad invoice", "ads invoice", "tax invoice for ads",
    "billing", "statement", "charged", "payment for ads",
    "ads account", "ad account", "invoice for advertising",
    "โฆษณา", "ค่าโฆษณา", "ยิงแอด", "บิลโฆษณา",
)

# Negative shipping/tracking context (prevent false ads classification)
NEGATIVE_FOR_ADS = (
    "address", "shipment", "shipping", "tracking", "waybill",
    "parcel", "ผู้รับ", "ที่อยู่", "ขนส่ง", "พัสดุ",
)

# Generic invoice signals
INVOICE_SIGS = (
    "ใบกำกับภาษี", "tax invoice", "receipt", "ใบเสร็จ", "invoice",
)

# Known client tax IDs (to exclude from vendor detection)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _norm(s: str) -> str:
    """Normalize + lower + trim; keep it safe"""
    try:
        t = normalize_text(s or "")
        t = t.lower()
        if len(t) > 160_000:
            # Head+tail window to keep speed stable
            t = t[:100_000] + "\n...\n" + t[-40_000:]
        return t
    except Exception as e:
        logger.warning(f"Normalization error: {e}")
        return ""


def _contains_any(t: str, needles: tuple[str, ...]) -> bool:
    """Check if text contains any of the needles"""
    return any(n and (n in t) for n in needles)


def _count_contains(t: str, needles: tuple[str, ...]) -> int:
    """Count how many needles are in text"""
    hit = 0
    for n in needles:
        if n and (n in t):
            hit += 1
    return hit


def _regex_hit(t: str, rx: re.Pattern) -> bool:
    """Safe regex search"""
    try:
        return rx.search(t) is not None
    except Exception:
        return False


def _has_vendor_tax_id(t: str) -> bool:
    """
    ✅ Check if text has 13-digit tax ID that is NOT client's
    (Strong indicator of Thai Tax Invoice)
    """
    try:
        for m in RE_TAX_ID_13.finditer(t):
            tax = m.group(1)
            if tax not in CLIENT_TAX_IDS:
                return True
        return False
    except Exception:
        return False


def _weighted_score(t: str, filename: str) -> Dict[str, int]:
    """
    ✅ Weighted scoring for 8 platforms using BOTH text and filename
    
    Returns:
        Dict with scores for each platform
    """
    fn = _norm(filename)
    tt = t

    score = {
        "META": 0,
        "GOOGLE": 0,
        "SHOPEE": 0,
        "LAZADA": 0,
        "TIKTOK": 0,
        "SPX": 0,
        "THAI_TAX": 0,
    }

    # ========== Priority 1: Meta/Facebook Ads (Highest) ==========
    # Meta receipt ID (RCMETA...)
    if _regex_hit(tt, RE_META_RECEIPT) or _regex_hit(fn, RE_META_RECEIPT):
        score["META"] += 150
    
    # Meta Platforms Ireland (very strong)
    if _regex_hit(tt, RE_META_IRELAND) or _regex_hit(fn, RE_META_IRELAND):
        score["META"] += 140
    
    # Facebook/Instagram ads
    if _regex_hit(tt, RE_FACEBOOK) or _regex_hit(fn, RE_FACEBOOK):
        score["META"] += 80
    
    # Meta signals
    score["META"] += 15 * _count_contains(tt, META_SIGS_STRONG)
    score["META"] += 20 * _count_contains(fn, META_SIGS_STRONG)
    score["META"] += 8 * _count_contains(tt, META_SIGS_WEAK)
    score["META"] += 10 * _count_contains(fn, META_SIGS_WEAK)

    # ========== Priority 2: Google Ads ==========
    # Google payment number (V...)
    if _regex_hit(tt, RE_GOOGLE_PAYMENT) or _regex_hit(fn, RE_GOOGLE_PAYMENT):
        score["GOOGLE"] += 150
    
    # Google Asia Pacific (very strong)
    if _regex_hit(tt, RE_GOOGLE_ASIA) or _regex_hit(fn, RE_GOOGLE_ASIA):
        score["GOOGLE"] += 140
    
    # Google Ads patterns
    if _regex_hit(tt, RE_GOOGLE_ADS) or _regex_hit(fn, RE_GOOGLE_ADS):
        score["GOOGLE"] += 80
    
    # Google signals
    score["GOOGLE"] += 15 * _count_contains(tt, GOOGLE_SIGS_STRONG)
    score["GOOGLE"] += 20 * _count_contains(fn, GOOGLE_SIGS_STRONG)
    score["GOOGLE"] += 8 * _count_contains(tt, GOOGLE_SIGS_WEAK)
    score["GOOGLE"] += 10 * _count_contains(fn, GOOGLE_SIGS_WEAK)

    # ========== Priority 3: SPX (Before Shopee!) ==========
    # SPX strongest: RCSPX with whitespace tolerance
    if _regex_hit(tt, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX):
        score["SPX"] += 120
    
    if "rcspx" in tt or "rcspx" in fn:
        score["SPX"] += 120
    
    # SPX signals
    score["SPX"] += 8 * _count_contains(tt, SPX_SIGS)
    score["SPX"] += 12 * _count_contains(fn, SPX_SIGS)

    # ========== Priority 4: Marketplace (Lazada, TikTok, Shopee) ==========
    # Lazada: THMPTI
    if _regex_hit(tt, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
        score["LAZADA"] += 100
    score["LAZADA"] += 8 * _count_contains(tt, LAZADA_SIGS)
    score["LAZADA"] += 12 * _count_contains(fn, LAZADA_SIGS)

    # TikTok: TTSTH
    if _regex_hit(tt, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH):
        score["TIKTOK"] += 100
    score["TIKTOK"] += 8 * _count_contains(tt, TIKTOK_SIGS)
    score["TIKTOK"] += 12 * _count_contains(fn, TIKTOK_SIGS)

    # Shopee: TIV-/TIR-
    if _regex_hit(tt, RE_SHOPEE_TIV) or _regex_hit(fn, RE_SHOPEE_TIV):
        score["SHOPEE"] += 90
    if _regex_hit(tt, RE_SHOPEE_TIR) or _regex_hit(fn, RE_SHOPEE_TIR):
        score["SHOPEE"] += 90
    if "shopee-ti" in tt or "shopee-ti" in fn:
        score["SHOPEE"] += 80
    
    # Shopee signals
    score["SHOPEE"] += 8 * _count_contains(tt, SHOPEE_SIGS)
    score["SHOPEE"] += 12 * _count_contains(fn, SHOPEE_SIGS)

    # TRS handling (Shopee weak - only count with context)
    trs_in_text = _regex_hit(tt, RE_SHOPEE_TRS) or ("trs" in tt)
    if trs_in_text:
        has_shopee_context = (
            ("shopee" in tt) or ("tiv" in tt) or ("tir" in tt) or
            ("shopee" in fn) or ("tiv" in fn) or ("tir" in fn)
        )
        if has_shopee_context:
            score["SHOPEE"] += 15

    # ========== Priority 5: Thai Tax Invoice ==========
    # Thai tax invoice patterns
    if _regex_hit(tt, RE_THAI_TAX_INVOICE):
        score["THAI_TAX"] += 50
    
    # Has vendor tax ID (not client's)
    if _has_vendor_tax_id(tt):
        score["THAI_TAX"] += 60
    
    # Has branch code
    if _regex_hit(tt, RE_BRANCH_5):
        score["THAI_TAX"] += 30
    
    # Thai tax signals
    score["THAI_TAX"] += 10 * _count_contains(tt, THAI_TAX_SIGS)

    # Penalty: If looks like Meta/Google/SPX/Marketplace, reduce Thai Tax score
    if score["META"] > 50 or score["GOOGLE"] > 50 or score["SPX"] > 50:
        score["THAI_TAX"] = int(score["THAI_TAX"] * 0.3)
    elif score["SHOPEE"] > 40 or score["LAZADA"] > 40 or score["TIKTOK"] > 40:
        score["THAI_TAX"] = int(score["THAI_TAX"] * 0.5)

    return score


def classify_platform(
    text: str,
    filename: str = "",
    debug: bool = False
) -> PlatformLabel:
    """
    ✅ Enhanced platform classifier for 8 platforms
    
    Args:
        text: PDF text content (OCR)
        filename: Original filename (highly informative!)
        debug: Enable debug logging
    
    Returns:
        Platform label (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
    
    Priority (highest to lowest):
    1. META (Meta/Facebook Ads) - receipt ID, Ireland, strong signals
    2. GOOGLE (Google Ads) - payment number, Asia Pacific, strong signals
    3. SPX (Shopee Express) - RCSPX pattern
    4. Marketplace (Lazada, TikTok, Shopee) - strong IDs
    5. THAI_TAX (Thai Tax Invoice) - tax ID + invoice patterns
    6. UNKNOWN (fallback)
    
    Features:
    ✅ Accept filename (queue filenames are highly informative)
    ✅ Priority-based detection (prevent misclassification)
    ✅ Meta/Google Ads detection
    ✅ Thai Tax Invoice detection
    ✅ Relaxed thresholds for better recall
    ✅ Comprehensive error handling
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    
    try:
        t = _norm(text)
        fn = _norm(filename)

        if not t and not fn:
            logger.debug("Empty text and filename -> UNKNOWN")
            return "UNKNOWN"

        logger.debug(f"Classifying: {filename[:50] if filename else 'no filename'}...")

        # ========== Fast Path (Strong IDs) ==========
        # Priority 1: Meta Ads (highest)
        if (
            _regex_hit(t, RE_META_RECEIPT) or _regex_hit(fn, RE_META_RECEIPT) or
            _regex_hit(t, RE_META_IRELAND) or _regex_hit(fn, RE_META_IRELAND)
        ):
            logger.info("✅ Fast path: META (Meta Platforms Ireland / RCMETA)")
            return "META"
        
        # Priority 2: Google Ads
        if (
            _regex_hit(t, RE_GOOGLE_PAYMENT) or _regex_hit(fn, RE_GOOGLE_PAYMENT) or
            _regex_hit(t, RE_GOOGLE_ASIA) or _regex_hit(fn, RE_GOOGLE_ASIA)
        ):
            logger.info("✅ Fast path: GOOGLE (Google Asia Pacific / Payment)")
            return "GOOGLE"
        
        # Priority 3: SPX (before Shopee!)
        if (
            _regex_hit(t, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX) or
            ("rcspx" in t) or ("rcspx" in fn)
        ):
            logger.info("✅ Fast path: SPX (RCSPX pattern)")
            return "SPX"
        
        # Priority 4: Lazada
        if _regex_hit(t, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
            logger.info("✅ Fast path: LAZADA (THMPTI pattern)")
            return "LAZADA"
        
        # Priority 5: TikTok
        if _regex_hit(t, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH):
            logger.info("✅ Fast path: TIKTOK (TTSTH pattern)")
            return "TIKTOK"

        # ========== Weighted Scoring ==========
        score = _weighted_score(t, filename=filename)
        
        logger.debug(f"Scores: {score}")

        # Resolve winner by score
        best_label = max(score.items(), key=lambda kv: kv[1])[0]
        best_score = score[best_label]

        # ========== Thresholds (Priority-Based) ==========
        
        # Priority 1: Meta Ads (threshold: 50)
        if score["META"] >= 50:
            logger.info(f"✅ Classification: META (score: {score['META']})")
            return "META"
        
        # Priority 2: Google Ads (threshold: 50)
        if score["GOOGLE"] >= 50:
            logger.info(f"✅ Classification: GOOGLE (score: {score['GOOGLE']})")
            return "GOOGLE"
        
        # Priority 3: SPX (threshold: 40, relaxed from 80)
        if score["SPX"] >= 40:
            logger.info(f"✅ Classification: SPX (score: {score['SPX']})")
            return "SPX"
        
        # Priority 4: Marketplace (thresholds: 40/30/30, relaxed)
        if score["LAZADA"] >= 40:
            logger.info(f"✅ Classification: LAZADA (score: {score['LAZADA']})")
            return "LAZADA"
        
        if score["TIKTOK"] >= 30:
            logger.info(f"✅ Classification: TIKTOK (score: {score['TIKTOK']})")
            return "TIKTOK"
        
        if score["SHOPEE"] >= 30:
            logger.info(f"✅ Classification: SHOPEE (score: {score['SHOPEE']})")
            return "SHOPEE"
        
        # Priority 5: Thai Tax Invoice (threshold: 60)
        # Only if clearly has tax ID + invoice patterns
        if score["THAI_TAX"] >= 60:
            logger.info(f"✅ Classification: THAI_TAX (score: {score['THAI_TAX']})")
            return "THAI_TAX"

        # ========== Fallback: Modest Confidence ==========
        if best_score >= 25:
            logger.info(f"⚠️  Modest confidence: {best_label} (score: {best_score})")
            return best_label  # type: ignore[return-value]

        # ========== Final Fallback: Generic Invoice ==========
        # If has invoice patterns but no clear platform, it's likely Thai Tax
        if _contains_any(t, INVOICE_SIGS) and _has_vendor_tax_id(t):
            logger.info("⚠️  Has invoice + vendor tax ID -> THAI_TAX")
            return "THAI_TAX"
        
        if _contains_any(t, INVOICE_SIGS) or _contains_any(fn, INVOICE_SIGS):
            logger.info("⚠️  Generic invoice -> UNKNOWN")
            return "UNKNOWN"

        logger.info(f"❌ UNKNOWN platform (scores: {score})")
        return "UNKNOWN"
    
    except Exception as e:
        logger.error(f"Classification error: {e}", exc_info=True)
        return "UNKNOWN"


def get_classification_details(
    text: str,
    filename: str = ""
) -> Tuple[PlatformLabel, Dict[str, int]]:
    """
    ✅ Get classification result WITH scores (for debugging)
    
    Args:
        text: PDF text content
        filename: Original filename
    
    Returns:
        Tuple of (platform, scores_dict)
    
    Example:
        platform, scores = get_classification_details(text, "meta_receipt.pdf")
        print(f"Platform: {platform}")
        print(f"Scores: {scores}")
        # Output:
        # Platform: META
        # Scores: {'META': 150, 'GOOGLE': 0, 'SHOPEE': 0, ...}
    """
    try:
        t = _norm(text)
        fn = _norm(filename)
        
        # Get scores
        score = _weighted_score(t, filename=filename)
        
        # Get classification
        platform = classify_platform(text, filename, debug=False)
        
        return (platform, score)
    
    except Exception as e:
        logger.error(f"Error getting classification details: {e}")
        return (
            "UNKNOWN",
            {
                "META": 0,
                "GOOGLE": 0,
                "SHOPEE": 0,
                "LAZADA": 0,
                "TIKTOK": 0,
                "SPX": 0,
                "THAI_TAX": 0,
            }
        )


def get_platform_metadata(platform: PlatformLabel) -> Dict[str, str]:
    """
    ✅ Get platform metadata (for integration with export_service/ai_service)
    
    Args:
        platform: Platform label
    
    Returns:
        Dict with platform metadata
    
    Example:
        meta = get_platform_metadata("META")
        # Returns:
        # {
        #     "vendor_code": "Meta Platforms Ireland",
        #     "vat_rate": "NO",
        #     "price_type": "3",
        #     "group": "Advertising Expense"
        # }
    """
    # Integration with export_service/ai_service constants
    metadata = {
        "META": {
            "vendor_code": "Meta Platforms Ireland",
            "vat_rate": "NO",
            "price_type": "3",
            "group": "Advertising Expense",
        },
        "GOOGLE": {
            "vendor_code": "Google Asia Pacific",
            "vat_rate": "NO",
            "price_type": "3",
            "group": "Advertising Expense",
        },
        "SHOPEE": {
            "vendor_code": "Shopee",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "LAZADA": {
            "vendor_code": "Lazada",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "TIKTOK": {
            "vendor_code": "TikTok",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "SPX": {
            "vendor_code": "Shopee Express",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Delivery/Logistics Expense",
        },
        "THAI_TAX": {
            "vendor_code": "",  # Variable (from document)
            "vat_rate": "7%",
            "price_type": "1",
            "group": "General Expense",
        },
        "UNKNOWN": {
            "vendor_code": "Other",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Other Expense",
        },
    }
    
    return metadata.get(platform, metadata["UNKNOWN"])


__all__ = [
    "PlatformLabel",
    "classify_platform",
    "get_classification_details",
    "get_platform_metadata",
]