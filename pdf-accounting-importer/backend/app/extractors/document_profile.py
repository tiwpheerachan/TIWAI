# -*- coding: utf-8 -*-
"""
document_profile.py (FIXED VERSION)

Improvements:
1. ✅ Added SPX detection
2. ✅ Added Thai Tax Invoice detection
3. ✅ Better Thai language support (fixed encoding)
4. ✅ Better error handling
5. ✅ More comprehensive platform detection
6. ✅ Enhanced Meta/Google Ads patterns

Create lightweight profiles for each page (keywords, ids, hints) and for segments.
Used by multi_page_analyzer + ai_document_router.

Design goals:
- No heavy dependencies besides stdlib
- Pure text heuristics (safe)
- Deterministic and debuggable
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# -----------------------------
# ✅ Regex helpers (with error handling)
# -----------------------------
try:
    RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")
    RE_SELLER_ID = re.compile(r"(?:seller\s*id|sellerid)\s*[:#]?\s*([0-9]{6,20})", re.IGNORECASE)
    RE_TRANSACTION = re.compile(r"(?:transaction|txn|ref)\s*[:#]?\s*([0-9]{6,30})", re.IGNORECASE)
    RE_INVOICE_NO = re.compile(
        r"(?:invoice\s*(?:no|number)|เลขที่ใบกำกับ|เลขที่เอกสาร|เลขที่)\s*[:#]?\s*([A-Z0-9\-/]{4,})",
        re.IGNORECASE
    )
    RE_PAGE_X_OF_Y = re.compile(r"\bpage\s*(\d{1,3})\s*(?:/|of)\s*(\d{1,3})\b", re.IGNORECASE)
except Exception as e:
    logger.error(f"Regex compilation failed: {e}")
    # Fallback simple patterns
    RE_TAX_ID_13 = re.compile(r"(\d{13})")
    RE_SELLER_ID = re.compile(r"seller.*?([0-9]{6,20})", re.IGNORECASE)
    RE_TRANSACTION = re.compile(r"transaction.*?([0-9]{6,30})", re.IGNORECASE)
    RE_INVOICE_NO = re.compile(r"invoice.*?([A-Z0-9\-/]{4,})", re.IGNORECASE)
    RE_PAGE_X_OF_Y = re.compile(r"page\s*(\d+)", re.IGNORECASE)


# -----------------------------
# ✅ Platform keywords (expanded)
# -----------------------------

KEYS_META = [
    "meta", "facebook", "ads manager", "business suite", "receipt",
    "account id", "transaction id", "meta platforms ireland",
    "fbads", "fb ads", "instagram ads",
]

KEYS_GOOGLE = [
    "google", "adwords", "google ads", "payment", "invoice",
    "google asia pacific", "billing id", "payment number",
    "google advertising", "google merchant",
]

KEYS_SHOPEE = [
    "shopee", "ช้อปปี้", "shopee (thailand)", "marketplace",
    "seller id", "คำสั่งซื้อ", "shopeepay", "shopee.co.th",
]

KEYS_LAZADA = [
    "lazada", "ลาซาด้า", "alibaba", "seller center",
    "lazpay", "lazada.co.th",
]

KEYS_TIKTOK = [
    "tiktok", "tik tok", "tts", "tiktok shop",
    "bytedance", "douyin",
]

# ✅ NEW: SPX patterns
KEYS_SPX = [
    "shopee express", "spx", "shopee logistics",
    "ขนส่งช้อปปี้", "spx express",
]

# ✅ NEW: Thai Tax Invoice patterns
KEYS_THAI_TAX = [
    "ใบเสร็จรับเงิน", "ใบกำกับภาษี",
    "ใบกำกับภาษีเต็มรูป", "tax invoice",
    "เลขประจำตัวผู้เสียภาษี",
    "รวมยอดที่ต้ระ", "ภาษีมูลค่าเพิ่ม",
]


def _norm_text(s: str) -> str:
    """Normalize text safely"""
    try:
        return (s or "").replace("\x00", " ").strip()
    except Exception as e:
        logger.warning(f"Text normalization failed: {e}")
        return str(s or "")


def _contains_any(t: str, keys: List[str]) -> bool:
    """Check if text contains any of the keywords"""
    try:
        tt = (t or "").lower()
        for k in keys:
            if k.lower() in tt:
                return True
        return False
    except Exception as e:
        logger.warning(f"Keyword matching failed: {e}")
        return False


def detect_platform_hint(text: str, filename: str = "") -> str:
    """
    ✅ IMPROVED: Better platform detection
    
    Return: "META" | "GOOGLE" | "SHOPEE" | "LAZADA" | "TIKTOK" | "SPX" | "THAI_TAX" | "UNKNOWN"
    """
    try:
        t = (text or "").lower()
        fn = (filename or "").lower()

        # ============================================================
        # STEP 1: Filename hint (fast path)
        # ============================================================
        
        if "meta" in fn or "facebook" in fn or "fbads" in fn:
            return "META"
        if "google" in fn or "adwords" in fn:
            return "GOOGLE"
        
        # ✅ SPX before Shopee (important!)
        if "spx" in fn or "express" in fn:
            return "SPX"
        
        if "shopee" in fn:
            return "SHOPEE"
        if "lazada" in fn or "laz" in fn:
            return "LAZADA"
        if "tiktok" in fn or "tts" in fn:
            return "TIKTOK"
        
        # ✅ Thai tax
        if "tax" in fn or "invoice" in fn or "receipt" in fn:
            # Need content check to confirm
            pass

        # ============================================================
        # STEP 2: Content hint (priority order matters!)
        # ============================================================
        
        # ✅ ADS first (high priority)
        if _contains_any(t, KEYS_META):
            return "META"
        if _contains_any(t, KEYS_GOOGLE):
            return "GOOGLE"
        
        # ✅ Thai Tax Invoice (medium priority)
        if _contains_any(t, KEYS_THAI_TAX):
            # Additional validation: must have 13-digit tax ID
            if re.search(r"\d{13}", t):
                return "THAI_TAX"
        
        # ✅ SPX before Shopee (important!)
        if _contains_any(t, KEYS_SPX):
            return "SPX"
        
        # Marketplace
        if _contains_any(t, KEYS_SHOPEE):
            return "SHOPEE"
        if _contains_any(t, KEYS_LAZADA):
            return "LAZADA"
        if _contains_any(t, KEYS_TIKTOK):
            return "TIKTOK"

        return "UNKNOWN"
    
    except Exception as e:
        logger.error(f"Platform detection failed: {e}")
        return "UNKNOWN"


def extract_first_tax_id(text: str) -> str:
    """Extract first 13-digit Thai tax ID"""
    try:
        m = RE_TAX_ID_13.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Tax ID extraction failed: {e}")
        return ""


def extract_seller_id(text: str) -> str:
    """Extract seller ID"""
    try:
        m = RE_SELLER_ID.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Seller ID extraction failed: {e}")
        return ""


def extract_transaction_id(text: str) -> str:
    """Extract transaction ID"""
    try:
        m = RE_TRANSACTION.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Transaction ID extraction failed: {e}")
        return ""


def extract_invoice_no(text: str) -> str:
    """Extract invoice number"""
    try:
        m = RE_INVOICE_NO.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Invoice number extraction failed: {e}")
        return ""


def extract_page_x_of_y(text: str) -> Tuple[int, int]:
    """Extract page numbering (e.g., 'Page 1 of 3')"""
    try:
        m = RE_PAGE_X_OF_Y.search((text or "").lower())
        if not m:
            return (0, 0)
        try:
            return (int(m.group(1)), int(m.group(2)))
        except (ValueError, IndexError):
            return (0, 0)
    except Exception as e:
        logger.warning(f"Page number extraction failed: {e}")
        return (0, 0)


def guess_doc_kind(platform_hint: str, text: str) -> str:
    """
    ✅ IMPROVED: Better doc kind detection
    
    Rough doc kind used for grouping:
    - META_RECEIPT
    - GOOGLE_PAYMENT
    - MARKETPLACE_BILL
    - SPX_WAYBILL
    - THAI_TAX_INVOICE
    - GENERIC
    """
    try:
        t = (text or "").lower()
        p = (platform_hint or "UNKNOWN").upper()

        # Meta Ads
        if p == "META":
            if "receipt" in t or "transaction id" in t or "account id" in t:
                return "META_RECEIPT"
            return "META_DOC"

        # Google Ads
        if p == "GOOGLE":
            if "payment" in t or "visa" in t or "mastercard" in t or "invoice" in t:
                return "GOOGLE_PAYMENT"
            return "GOOGLE_DOC"

        # ✅ SPX
        if p == "SPX":
            if "waybill" in t or "tracking" in t:
                return "SPX_WAYBILL"
            return "SPX_DOC"

        # ✅ Thai Tax
        if p == "THAI_TAX":
            if "ใบกำกับภาษีเต็มรูป" in t or "tax invoice" in t:
                return "THAI_TAX_INVOICE"
            if "ใบเสร็จรับเงิน" in t or "receipt" in t:
                return "THAI_RECEIPT"
            return "THAI_TAX_DOC"

        # Marketplace
        if p in {"SHOPEE", "LAZADA", "TIKTOK"}:
            return "MARKETPLACE_BILL"

        return "GENERIC"
    
    except Exception as e:
        logger.error(f"Doc kind detection failed: {e}")
        return "GENERIC"


@dataclass
class PageProfile:
    """Profile for a single page"""
    page_index: int
    text_len: int
    platform_hint: str
    doc_kind: str
    tax_id_13: str = ""
    seller_id: str = ""
    transaction_id: str = ""
    invoice_no: str = ""
    page_x: int = 0
    page_y: int = 0
    keywords: List[str] = field(default_factory=list)

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        return {
            "page_index": self.page_index,
            "text_len": self.text_len,
            "platform_hint": self.platform_hint,
            "doc_kind": self.doc_kind,
            "tax_id_13": self.tax_id_13,
            "seller_id": self.seller_id,
            "transaction_id": self.transaction_id,
            "invoice_no": self.invoice_no,
            "page_x": self.page_x,
            "page_y": self.page_y,
            "keywords": self.keywords,
        }


@dataclass
class SegmentProfile:
    """Profile for a segment (multiple pages)"""
    segment_index: int
    page_indices: List[int]
    merged_text_len: int
    platform_hint: str
    doc_kind: str
    tax_id_13: str = ""
    seller_id: str = ""
    transaction_id: str = ""
    invoice_no: str = ""
    reasons: List[str] = field(default_factory=list)

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        return {
            "segment_index": self.segment_index,
            "page_indices": self.page_indices,
            "merged_text_len": self.merged_text_len,
            "platform_hint": self.platform_hint,
            "doc_kind": self.doc_kind,
            "tax_id_13": self.tax_id_13,
            "seller_id": self.seller_id,
            "transaction_id": self.transaction_id,
            "invoice_no": self.invoice_no,
            "reasons": self.reasons,
        }


def build_page_profile(page_index: int, page_text: str, filename: str = "") -> PageProfile:
    """
    ✅ IMPROVED: Build profile with better error handling
    """
    try:
        t = _norm_text(page_text)
        ph = detect_platform_hint(t, filename=filename)
        kind = guess_doc_kind(ph, t)

        tax = extract_first_tax_id(t)
        seller = extract_seller_id(t)
        txn = extract_transaction_id(t)
        inv = extract_invoice_no(t)
        px, py = extract_page_x_of_y(t)

        # Extract keywords
        keywords: List[str] = []
        all_keys = (
            KEYS_META + KEYS_GOOGLE + KEYS_SHOPEE +
            KEYS_LAZADA + KEYS_TIKTOK + KEYS_SPX + KEYS_THAI_TAX
        )
        
        try:
            tt = t.lower()
            for k in all_keys:
                if k.lower() in tt:
                    keywords.append(k)
        except Exception as e:
            logger.warning(f"Keyword extraction failed: {e}")

        return PageProfile(
            page_index=page_index,
            text_len=len(t),
            platform_hint=ph,
            doc_kind=kind,
            tax_id_13=tax,
            seller_id=seller,
            transaction_id=txn,
            invoice_no=inv,
            page_x=px,
            page_y=py,
            keywords=keywords[:30],  # Limit to 30 keywords
        )
    
    except Exception as e:
        logger.error(f"Page profile building failed: {e}")
        # Return minimal profile
        return PageProfile(
            page_index=page_index,
            text_len=len(page_text or ""),
            platform_hint="UNKNOWN",
            doc_kind="GENERIC",
        )


def merge_segment_profile(
    segment_index: int,
    pages: List[PageProfile],
    merged_text: str
) -> SegmentProfile:
    """
    ✅ IMPROVED: Merge multiple pages into segment profile
    """
    try:
        # platform/doc_kind: choose the most common non-UNKNOWN
        ph = "UNKNOWN"
        kind = "GENERIC"
        reasons: List[str] = []

        ph_counts: Dict[str, int] = {}
        kind_counts: Dict[str, int] = {}
        
        for p in pages:
            if p.platform_hint:
                ph_counts[p.platform_hint] = ph_counts.get(p.platform_hint, 0) + 1
            if p.doc_kind:
                kind_counts[p.doc_kind] = kind_counts.get(p.doc_kind, 0) + 1

        # Choose most common platform (prefer non-UNKNOWN)
        if ph_counts:
            # Sort by count (desc), then by name
            # Prefer non-UNKNOWN
            sorted_ph = sorted(
                ph_counts.items(),
                key=lambda x: (
                    -x[1] if x[0] != "UNKNOWN" else -x[1] - 1000,
                    x[0]
                )
            )
            ph = sorted_ph[0][0]
        
        # Choose most common doc kind
        if kind_counts:
            kind = sorted(kind_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

        # Extract IDs (take first non-empty)
        tax = ""
        seller = ""
        txn = ""
        inv = ""
        
        for p in pages:
            if not tax and p.tax_id_13:
                tax = p.tax_id_13
            if not seller and p.seller_id:
                seller = p.seller_id
            if not txn and p.transaction_id:
                txn = p.transaction_id
            if not inv and p.invoice_no:
                inv = p.invoice_no

        reasons.append(f"platform={ph} (counts={ph_counts})")
        reasons.append(f"doc_kind={kind} (counts={kind_counts})")

        return SegmentProfile(
            segment_index=segment_index,
            page_indices=[p.page_index for p in pages],
            merged_text_len=len(_norm_text(merged_text)),
            platform_hint=ph,
            doc_kind=kind,
            tax_id_13=tax,
            seller_id=seller,
            transaction_id=txn,
            invoice_no=inv,
            reasons=reasons,
        )
    
    except Exception as e:
        logger.error(f"Segment profile merging failed: {e}")
        # Return minimal profile
        return SegmentProfile(
            segment_index=segment_index,
            page_indices=[p.page_index for p in pages],
            merged_text_len=len(merged_text or ""),
            platform_hint="UNKNOWN",
            doc_kind="GENERIC",
            reasons=[f"error: {str(e)[:100]}"],
        )