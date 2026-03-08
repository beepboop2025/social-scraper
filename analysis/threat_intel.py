"""Threat intelligence analysis — classifies dark web and security content.

Designed for financial sector threat monitoring:
- Data breach detection for banks/exchanges
- Ransomware targeting financial institutions
- Credential leak monitoring
- Cryptocurrency theft/scam detection
- Regulatory/sanctions evasion schemes
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Threat categories
THREAT_CATEGORIES = {
    "data_breach": [
        "data breach", "leaked database", "dump", "exposed data",
        "data leak", "compromised accounts", "stolen data",
    ],
    "ransomware": [
        "ransomware", "encrypted files", "ransom demand",
        "lockbit", "blackcat", "cl0p", "play ransomware",
        "data extortion", "double extortion",
    ],
    "credential_theft": [
        "credential dump", "password leak", "login credentials",
        "combo list", "stealer logs", "infostealer",
        "redline", "raccoon stealer", "vidar",
    ],
    "financial_fraud": [
        "banking trojan", "payment fraud", "card skimmer",
        "atm malware", "pos malware", "money mule",
        "business email compromise", "bec",
    ],
    "crypto_threat": [
        "wallet drain", "exchange hack", "rug pull",
        "flash loan attack", "bridge exploit", "smart contract exploit",
        "crypto scam", "ponzi", "exit scam",
    ],
    "insider_threat": [
        "insider trading", "insider threat", "data exfiltration",
        "corporate espionage", "trade secret",
    ],
    "supply_chain": [
        "supply chain attack", "dependency confusion",
        "malicious package", "compromised update",
    ],
    "sanctions_evasion": [
        "sanctions evasion", "money laundering", "kyc bypass",
        "mixer", "tornado cash", "ofac",
    ],
}

# Financial institution patterns
FINANCIAL_TARGETS = re.compile(
    r"\b(bank|credit union|exchange|brokerage|insurance|fintech|"
    r"payment processor|clearing house|hedge fund|"
    r"sbi|hdfc|icici|axis|kotak|jpmorgan|goldman sachs|"
    r"morgan stanley|citibank|wells fargo|"
    r"binance|coinbase|kraken|bybit|okx|"
    r"visa|mastercard|paypal|stripe)\b",
    re.I,
)


def classify_threat(text: str) -> dict:
    """Classify a threat intelligence item.

    Returns:
        dict with threat_level, categories, financial_targets,
        confidence, and recommended_action.
    """
    text_lower = text.lower()

    # Find matching categories
    matched_categories = {}
    for category, keywords in THREAT_CATEGORIES.items():
        hits = [kw for kw in keywords if kw in text_lower]
        if hits:
            matched_categories[category] = hits

    # Find financial targets
    targets = FINANCIAL_TARGETS.findall(text)
    unique_targets = list(set(t.lower() for t in targets))

    # Calculate threat level
    category_count = len(matched_categories)
    has_financial_target = len(unique_targets) > 0

    if category_count >= 3 or (category_count >= 2 and has_financial_target):
        threat_level = "critical"
        confidence = 0.9
    elif category_count >= 2 or (category_count >= 1 and has_financial_target):
        threat_level = "high"
        confidence = 0.75
    elif category_count >= 1:
        threat_level = "medium"
        confidence = 0.6
    else:
        threat_level = "low"
        confidence = 0.3

    # Recommended action
    if threat_level == "critical":
        action = "IMMEDIATE: Alert security team, check if targets match our portfolio"
    elif threat_level == "high":
        action = "URGENT: Review within 1 hour, assess exposure to affected entities"
    elif threat_level == "medium":
        action = "MONITOR: Track for escalation, update threat feed"
    else:
        action = "LOG: Record for pattern analysis"

    return {
        "threat_level": threat_level,
        "categories": matched_categories,
        "financial_targets": unique_targets,
        "confidence": confidence,
        "recommended_action": action,
        "category_count": category_count,
        "has_financial_target": has_financial_target,
    }


def extract_threat_indicators(text: str) -> dict:
    """Extract technical indicators of compromise (IOCs)."""
    indicators = {
        "ips": list(set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)))[:10],
        "domains": list(set(re.findall(r"\b[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.(?:onion|com|net|org|io)\b", text)))[:10],
        "hashes_md5": list(set(re.findall(r"\b[a-fA-F0-9]{32}\b", text)))[:5],
        "hashes_sha256": list(set(re.findall(r"\b[a-fA-F0-9]{64}\b", text)))[:5],
        "btc_addresses": list(set(re.findall(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b", text)))[:5],
        "eth_addresses": list(set(re.findall(r"\b0x[a-fA-F0-9]{40}\b", text)))[:5],
        "emails": list(set(re.findall(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", text)))[:5],
        "cve_ids": list(set(re.findall(r"CVE-\d{4}-\d{4,}", text)))[:5],
    }

    indicators["total_count"] = sum(len(v) for v in indicators.values() if isinstance(v, list))
    return indicators


def analyze_threat(text: str) -> dict:
    """Full threat intelligence analysis."""
    classification = classify_threat(text)
    indicators = extract_threat_indicators(text)

    return {
        **classification,
        "indicators": indicators,
        "ioc_count": indicators["total_count"],
    }


def batch_threat_analysis(texts: list[str]) -> list[dict]:
    """Analyze a batch of texts for threat intelligence."""
    return [analyze_threat(t) for t in texts]
