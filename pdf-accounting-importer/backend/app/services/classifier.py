# backend/app/services/classifier.py
from __future__ import annotations

from typing import Literal

from ..utils.text_utils import normalize_text

PlatformLabel = Literal["shopee", "lazada", "tiktok", "spx", "ads", "other", "unknown"]

# ---------------------------------------------------------------------
# Regex / Signals (keep simple + robust)
# ---------------------------------------------------------------------

# Document id signals
SHOPEE_DOC_SIGS = ("shopee", "tiv-", "tir-", "trs", "shopee-ti", "shopee-tiv", "shopee-tir")
LAZADA_DOC_SIGS = ("lazada", "thmpti", "lzd", "laz", "lazada invoice")
TIKTOK_DOC_SIGS = ("tiktok", "ttsth", "tiktok shop")
SPX_DOC_SIGS = ("spx", "spx express", "standard express", "rcs", "rcspx")

# Ads / billing signals (platform ads / google/meta/tt ads)
ADS_SIGS = (
    "ads", "advertising", "ad account", "ad invoice", "ads invoice",
    "โฆษณา", "ค่าโฆษณา", "ยิงแอด",
    "facebook ads", "meta ads", "google ads", "tiktok ads", "line ads",
    "billing", "charged", "payment for ads",
)

# Generic invoice signals (fallback)
INVOICE_SIGS = (
    "ใบกำกับภาษี", "tax invoice", "receipt", "ใบเสร็จ", "invoice", "tax receipt",
)

# Extra negative signals to avoid false positives
NEGATIVE_FOR_ADS = (
    "address", "shipment", "shipping", "tracking", "waybill", "parcel",
    "ผู้รับ", "ที่อยู่", "ขนส่ง", "พัสดุ", "จัดส่ง",
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _contains_any(t: str, needles: tuple[str, ...]) -> bool:
    return any(n in t for n in needles)


def _score_signals(t: str, sigs: tuple[str, ...]) -> int:
    """Simple scoring: count matched signals (unique)."""
    hit = 0
    for s in sigs:
        if s and s in t:
            hit += 1
    return hit


def classify_platform(text: str) -> PlatformLabel:
    """
    Smart-ish platform classifier.

    Goals:
    - Avoid Shopee false positive from: "trs" (too common) unless paired with other Shopee signals
    - Detect TikTok by TTSTH or "tiktok"
    - Detect Lazada by THMPTI or "lazada"
    - Detect SPX by RCSPX / SPX Express
    - Detect Ads by strong ad/billing keywords (but avoid shipping/tracking context)
    - Fallback to "other" if looks like tax invoice but no platform signal
    """
    t = normalize_text(text).lower()
    if not t:
        return "unknown"

    # Keep only a reasonable amount for speed (head+tail)
    if len(t) > 120_000:
        t = t[:80_000] + "\n...\n" + t[-30_000:]

    # ---------------------------
    # Strong ID-based matches (highest confidence)
    # ---------------------------
    if "ttsth" in t:
        return "tiktok"
    if "thmpti" in t:
        return "lazada"
    # SPX specific doc id: RCSPX...
    if "rcspx" in t or ("spx" in t and "express" in t):
        return "spx"

    # Shopee TI* patterns
    if "tiv-" in t or "tir-" in t or "shopee-ti" in t:
        return "shopee"

    # ---------------------------
    # Ads detection (strong words + not shipment context)
    # ---------------------------
    ads_score = _score_signals(t, ADS_SIGS)
    if ads_score >= 2:
        # if it is clearly shipment related, don't mark ads
        if not _contains_any(t, NEGATIVE_FOR_ADS):
            return "ads"

    # ---------------------------
    # Platform keyword scoring
    # ---------------------------
    # Important: "trs" alone is NOT reliable (too many collisions).
    # We'll only count TRS as Shopee if ALSO there is another Shopee signal nearby.
    shopee_score = _score_signals(t, tuple(s for s in SHOPEE_DOC_SIGS if s != "trs"))
    lazada_score = _score_signals(t, LAZADA_DOC_SIGS)
    tiktok_score = _score_signals(t, TIKTOK_DOC_SIGS)
    spx_score = _score_signals(t, SPX_DOC_SIGS)

    # Handle TRS carefully:
    if "trs" in t:
        # Promote Shopee if there is any other Shopee-ish word
        if "shopee" in t or "tiv" in t or "tir" in t:
            shopee_score += 1

    # Choose best platform by score (>=1)
    best = max(
        (("shopee", shopee_score),
         ("lazada", lazada_score),
         ("tiktok", tiktok_score),
         ("spx", spx_score)),
        key=lambda x: x[1],
    )

    if best[1] >= 1:
        return best[0]  # type: ignore[return-value]

    # ---------------------------
    # Fallback: looks like invoice but unknown platform
    # ---------------------------
    if _contains_any(t, INVOICE_SIGS):
        return "other"

    return "unknown"


__all__ = ["PlatformLabel", "classify_platform"]
