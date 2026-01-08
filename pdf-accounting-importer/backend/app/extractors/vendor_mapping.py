# backend/app/extractors/vendor_mapping.py
"""
Vendor Code Mapping System
แมปรหัสคู่ค้าตาม Tax ID ของ client และ vendor platform
"""
from typing import Dict, Tuple, Optional

# ============================================================
# Client Tax ID Constants
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Vendor Tax ID Constants
# ============================================================
VENDOR_SHOPEE = "0105558019581"
VENDOR_LAZADA = "0105555040244"
VENDOR_TIKTOK = "0105566214176"
VENDOR_MARKETPLACE_OTHER = "0105548000241"
VENDOR_SHOPIFY = "0993000475879"
VENDOR_SPX = "0105561164871"

# ============================================================
# Vendor Code Mapping
# Key: (client_tax_id, vendor_tax_id)
# Value: vendor_code
# ============================================================
VENDOR_CODE_MAP: Dict[Tuple[str, str], str] = {
    # ===== Rabbit Client (0105561071873) =====
    (CLIENT_RABBIT, VENDOR_SHOPEE): "C00395",
    (CLIENT_RABBIT, VENDOR_LAZADA): "C00411",
    (CLIENT_RABBIT, VENDOR_TIKTOK): "C00562",
    (CLIENT_RABBIT, VENDOR_MARKETPLACE_OTHER): "C01031",
    (CLIENT_RABBIT, VENDOR_SHOPIFY): "C01143",
    (CLIENT_RABBIT, VENDOR_SPX): "C00563",
    
    # ===== SHD Client (0105563022918) =====
    (CLIENT_SHD, VENDOR_SHOPEE): "C00888",
    (CLIENT_SHD, VENDOR_LAZADA): "C01132",
    (CLIENT_SHD, VENDOR_TIKTOK): "C01246",
    (CLIENT_SHD, VENDOR_MARKETPLACE_OTHER): "C01420",
    (CLIENT_SHD, VENDOR_SHOPIFY): "C33491",
    (CLIENT_SHD, VENDOR_SPX): "C01133",
    
    # ===== TopOne Client (0105565027615) =====
    (CLIENT_TOPONE, VENDOR_SHOPEE): "C00020",
    (CLIENT_TOPONE, VENDOR_LAZADA): "C00025",
    (CLIENT_TOPONE, VENDOR_TIKTOK): "C00051",
    (CLIENT_TOPONE, VENDOR_MARKETPLACE_OTHER): "C00095",
    (CLIENT_TOPONE, VENDOR_SPX): "C00038",
}

# ============================================================
# Vendor Name Mapping (for fallback)
# ============================================================
VENDOR_NAME_MAP: Dict[str, str] = {
    "shopee": VENDOR_SHOPEE,
    "ช้อปปี้": VENDOR_SHOPEE,
    "lazada": VENDOR_LAZADA,
    "ลาซาด้า": VENDOR_LAZADA,
    "tiktok": VENDOR_TIKTOK,
    "ติ๊กต๊อก": VENDOR_TIKTOK,
    "spx": VENDOR_SPX,
    "spx express": VENDOR_SPX,
    "shopify": VENDOR_SHOPIFY,
    "marketplace": VENDOR_MARKETPLACE_OTHER,
}

# ============================================================
# Helper Functions
# ============================================================

def get_vendor_code(client_tax_id: str, vendor_tax_id: str, vendor_name: str = "") -> str:
    """
    Get vendor code based on client and vendor tax IDs
    
    Args:
        client_tax_id: Tax ID of the client (Rabbit/SHD/TopOne)
        vendor_tax_id: Tax ID of the vendor (Shopee/Lazada/TikTok/SPX)
        vendor_name: Vendor name (fallback if tax_id not found)
    
    Returns:
        Vendor code (e.g., C00395) or vendor_name if not found
    """
    # Normalize tax IDs
    client_tax_id = client_tax_id.strip() if client_tax_id else ""
    vendor_tax_id = vendor_tax_id.strip() if vendor_tax_id else ""
    
    # Try exact match
    key = (client_tax_id, vendor_tax_id)
    if key in VENDOR_CODE_MAP:
        return VENDOR_CODE_MAP[key]
    
    # Try to find vendor_tax_id from vendor_name
    if vendor_name:
        vendor_name_lower = vendor_name.lower().strip()
        for name_key, tax_id in VENDOR_NAME_MAP.items():
            if name_key in vendor_name_lower:
                key = (client_tax_id, tax_id)
                if key in VENDOR_CODE_MAP:
                    return VENDOR_CODE_MAP[key]
    
    # Fallback to vendor name or platform code
    if vendor_name:
        if "shopee" in vendor_name.lower() or "ช้อปปี้" in vendor_name:
            return "Shopee"
        elif "lazada" in vendor_name.lower() or "ลาซาด้า" in vendor_name:
            return "Lazada"
        elif "tiktok" in vendor_name.lower() or "ติ๊กต๊อก" in vendor_name:
            return "TikTok"
        elif "spx" in vendor_name.lower():
            return "SPX"
        return vendor_name
    
    return "Unknown"


def detect_client_from_context(text: str) -> Optional[str]:
    """
    Try to detect client Tax ID from document context
    
    This is a helper function to auto-detect which client
    the document belongs to based on content.
    
    Returns:
        Client Tax ID or None
    """
    t = text.lower()
    
    # Look for client mentions in document
    if "rabbit" in t or CLIENT_RABBIT in t:
        return CLIENT_RABBIT
    elif "shd" in t or CLIENT_SHD in t:
        return CLIENT_SHD
    elif "topone" in t or "top one" in t or CLIENT_TOPONE in t:
        return CLIENT_TOPONE
    
    return None


def get_client_name(client_tax_id: str) -> str:
    """Get client name from Tax ID"""
    if client_tax_id == CLIENT_RABBIT:
        return "Rabbit"
    elif client_tax_id == CLIENT_SHD:
        return "SHD"
    elif client_tax_id == CLIENT_TOPONE:
        return "TopOne"
    return "Unknown"


def get_all_vendor_codes_for_client(client_tax_id: str) -> Dict[str, str]:
    """
    Get all vendor codes for a specific client
    
    Returns:
        Dict mapping vendor_tax_id to vendor_code
    """
    result = {}
    for (client, vendor), code in VENDOR_CODE_MAP.items():
        if client == client_tax_id:
            result[vendor] = code
    return result


# ============================================================
# Category Mapping for Description
# ============================================================

CATEGORY_DESCRIPTION_MAP = {
    "marketplace": "Marketplace Expense",
    "commission": "Selling Expense",
    "advertising": "Advertising Expense",
    "goods": "Inventory / COGS",
    "shipping": "Shipping Expense",
}


def get_expense_category(description: str, platform: str = "") -> str:
    """
    Get expense category based on description and platform
    
    Args:
        description: Fee description
        platform: Platform name (Shopee/Lazada/TikTok/SPX)
    
    Returns:
        Category name
    """
    desc_lower = description.lower()
    
    # Platform fees → Marketplace Expense
    if platform.lower() in ['shopee', 'lazada', 'tiktok', 'ช้อปปี้', 'ลาซาด้า', 'ติ๊กต๊อก']:
        return "Marketplace Expense"
    
    # Commission → Selling Expense
    if any(word in desc_lower for word in ['commission', 'คอมมิชชั่น', 'ค่าคอมฯ']):
        return "Selling Expense"
    
    # Advertising → Advertising Expense
    if any(word in desc_lower for word in ['advertising', 'โฆษณา', 'ads', 'sponsored']):
        return "Advertising Expense"
    
    # Goods → Inventory / COGS
    if any(word in desc_lower for word in ['goods', 'สินค้า', 'inventory', 'cogs']):
        return "Inventory / COGS"
    
    # Shipping → Shipping Expense
    if any(word in desc_lower for word in ['shipping', 'delivery', 'ขนส่ง', 'จัดส่ง', 'spx']):
        return "Shipping Expense"
    
    # Default
    return "Marketplace Expense"


def format_short_description(platform: str, fee_type: str = "", seller_info: str = "") -> str:
    """
    Format short description for PEAK import
    
    Args:
        platform: Platform name
        fee_type: Type of fee (optional)
        seller_info: Seller information (optional)
    
    Returns:
        Short formatted description
    """
    parts = []
    
    # Platform
    if platform:
        parts.append(platform)
    
    # Fee type
    if fee_type:
        parts.append(fee_type)
    
    # Seller info (short)
    if seller_info:
        # Extract just seller ID if available
        if "Seller ID:" in seller_info or "Seller:" in seller_info:
            import re
            m = re.search(r'Seller(?:\s+ID)?:\s*(\w+)', seller_info)
            if m:
                parts.append(f"Seller {m.group(1)}")
        else:
            parts.append(seller_info[:20])  # Limit to 20 chars
    
    return " - ".join(parts) if parts else "Marketplace Expense"


# ============================================================
# Export
# ============================================================

__all__ = [
    'get_vendor_code',
    'detect_client_from_context',
    'get_client_name',
    'get_all_vendor_codes_for_client',
    'get_expense_category',
    'format_short_description',
    'CLIENT_RABBIT',
    'CLIENT_SHD',
    'CLIENT_TOPONE',
    'VENDOR_SHOPEE',
    'VENDOR_LAZADA',
    'VENDOR_TIKTOK',
    'VENDOR_SPX',
]