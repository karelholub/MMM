from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ChannelRule:
    """Rule to map raw source/medium to a standard channel."""

    name: str
    channel: str
    source_regex: Optional[str] = None
    medium_regex: Optional[str] = None

    def matches(self, source: str, medium: str) -> bool:
        if self.source_regex and not re.search(self.source_regex, source or "", re.IGNORECASE):
            return False
        if self.medium_regex and not re.search(self.medium_regex, medium or "", re.IGNORECASE):
            return False
        return True


@dataclass
class Taxonomy:
    channel_rules: List[ChannelRule] = field(default_factory=list)
    source_aliases: Dict[str, str] = field(default_factory=dict)
    medium_aliases: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def default(cls) -> "Taxonomy":
        return cls(
            channel_rules=[
                ChannelRule(name="Paid Search", channel="paid_search", source_regex="google|bing|baidu", medium_regex="cpc|ppc|paid_search"),
                ChannelRule(name="Paid Social", channel="paid_social", source_regex="facebook|meta|instagram|linkedin|twitter|tiktok", medium_regex="cpc|paid_social|social"),
                ChannelRule(name="Email", channel="email", medium_regex="email"),
                ChannelRule(name="Direct", channel="direct", medium_regex="(none|direct)"),
            ],
            source_aliases={"fb": "facebook", "ig": "instagram", "g": "google"},
            medium_aliases={"paid": "cpc", "cpm": "display"},
        )


_BASE_DIR = Path(__file__).resolve().parent.parent


def _taxonomy_path() -> Path:
    # data/ directory at app root (sibling of this module's package)
    data_dir = _BASE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "taxonomy.json"


def load_taxonomy() -> Taxonomy:
    path = _taxonomy_path()
    if not path.exists():
        tax = Taxonomy.default()
        save_taxonomy(tax)
        return tax
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return Taxonomy.default()

    rules = []
    for r in raw.get("channel_rules", []):
        rules.append(
            ChannelRule(
                name=r.get("name", ""),
                channel=r.get("channel", ""),
                source_regex=r.get("source_regex"),
                medium_regex=r.get("medium_regex"),
            )
        )
    return Taxonomy(
        channel_rules=rules,
        source_aliases=raw.get("source_aliases", {}),
        medium_aliases=raw.get("medium_aliases", {}),
    )


def save_taxonomy(taxonomy: Taxonomy) -> None:
    path = _taxonomy_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "channel_rules": [
            {
                "name": r.name,
                "channel": r.channel,
                "source_regex": r.source_regex,
                "medium_regex": r.medium_regex,
            }
            for r in taxonomy.channel_rules
        ],
        "source_aliases": taxonomy.source_aliases,
        "medium_aliases": taxonomy.medium_aliases,
    }
    path.write_text(json.dumps(payload, indent=2))


def normalize_touchpoint(tp: Dict[str, Any], taxonomy: Optional[Taxonomy] = None) -> Dict[str, Any]:
    if taxonomy is None:
        taxonomy = load_taxonomy()

    # raw inputs
    raw_source = str(tp.get("utm_source") or tp.get("source") or "").lower()
    raw_medium = str(tp.get("utm_medium") or tp.get("medium") or "").lower()

    source = taxonomy.source_aliases.get(raw_source, raw_source)
    medium = taxonomy.medium_aliases.get(raw_medium, raw_medium)

    # channel mapping
    channel = str(tp.get("channel") or "")
    for rule in taxonomy.channel_rules:
        if rule.matches(source, medium):
            channel = rule.channel
            break

    campaign = tp.get("campaign") or tp.get("utm_campaign") or None
    adset = tp.get("adset") or None
    ad = tp.get("ad") or None
    creative = tp.get("creative") or tp.get("utm_content") or None

    normalized = dict(tp)
    if channel:
        normalized["channel"] = channel
    if source:
        normalized["source"] = source
    if medium:
        normalized["medium"] = medium
    if campaign is not None:
        normalized["campaign"] = campaign
    if adset is not None:
        normalized["adset"] = adset
    if ad is not None:
        normalized["ad"] = ad
    if creative is not None:
        normalized["creative"] = creative

    return normalized

