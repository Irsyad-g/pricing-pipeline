COMMISSION = {
    ("TikTok Tokopedia GK", "eSIM"):    8.0,
    ("TikTok Tokopedia GK", "Simcard"): 12.0,
    ("Shopee GK",           "eSIM"):    9.0,
    ("Shopee GK",           "Simcard"): 9.0,
    ("Shopify GK",          "eSIM"):    0.0,
    ("Shopify GK",          "Simcard"): 0.0,
}

DEFAULT_COMMISSION = 0.0


def get_commission(channel: str, product_type: str) -> float:
    channel      = str(channel).strip()
    product_type = str(product_type).strip()
    
    # coba exact match dulu
    pct = COMMISSION.get((channel, product_type))
    if pct is not None:
        return pct / 100
    
    # fallback — case insensitive match
    for (ch, pt), val in COMMISSION.items():
        if ch.lower() == channel.lower() and pt.lower() == product_type.lower():
            return val / 100
    
    return DEFAULT_COMMISSION