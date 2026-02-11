from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class MatchExpression:
    """Declarative match expression used for taxonomy rule conditions."""

    operator: str = "any"  # any / contains / equals / regex
    value: str = ""

    def normalize_operator(self) -> str:
        op = (self.operator or "any").lower()
        if op not in {"contains", "equals", "regex", "any"}:
            return "any"
        return op


def _evaluate_expression(expr: MatchExpression, text: Optional[str]) -> bool:
    operator = expr.normalize_operator()
    value = expr.value or ""
    candidate = (text or "").lower()
    if operator == "any" or not value:
        return True

    if operator == "contains":
        return value.lower() in candidate

    if operator == "equals":
        return value.lower() == candidate

    if operator == "regex":
        try:
            return re.search(value, text or "", re.IGNORECASE) is not None
        except re.error:
            return False

    return True


@dataclass
class ChannelRule:
    """Rule to map raw source/medium to a standard channel."""

    name: str
    channel: str
    priority: int = 100
    enabled: bool = True
    source: MatchExpression = field(default_factory=MatchExpression)
    medium: MatchExpression = field(default_factory=MatchExpression)
    campaign: MatchExpression = field(default_factory=MatchExpression)

    def matches(self, source: str, medium: str, campaign: Optional[str] = None) -> bool:
        if not self.enabled:
            return False

        if not _evaluate_expression(self.source, source):
            return False

        if not _evaluate_expression(self.medium, medium):
            return False

        if not _evaluate_expression(self.campaign, campaign or ""):
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
                ChannelRule(
                    name="Paid Search",
                    channel="paid_search",
                    priority=10,
                    source=MatchExpression(operator="regex", value="google|bing|baidu"),
                    medium=MatchExpression(operator="regex", value="cpc|ppc|paid_search"),
                ),
                ChannelRule(
                    name="Paid Social",
                    channel="paid_social",
                    priority=20,
                    source=MatchExpression(operator="regex", value="facebook|meta|instagram|linkedin|twitter|tiktok"),
                    medium=MatchExpression(operator="regex", value="cpc|paid_social|social"),
                ),
                ChannelRule(
                    name="Email",
                    channel="email",
                    priority=30,
                    medium=MatchExpression(operator="contains", value="email"),
                ),
                ChannelRule(
                    name="Direct",
                    channel="direct",
                    priority=40,
                    medium=MatchExpression(operator="regex", value="(none|direct)"),
                ),
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

    def _parse_expression(data: Optional[Dict[str, Any]], fallback_regex: Optional[str] = None) -> MatchExpression:
        if isinstance(data, dict):
            operator = data.get("operator", "any")
            value = data.get("value", "")
            return MatchExpression(operator=operator, value=value or "")
        if fallback_regex:
            return MatchExpression(operator="regex", value=fallback_regex)
        return MatchExpression()

    rules: List[ChannelRule] = []
    for idx, r in enumerate(raw.get("channel_rules", [])):
        rules.append(
            ChannelRule(
                name=r.get("name", ""),
                channel=r.get("channel", ""),
                priority=int(r.get("priority", (idx + 1) * 10)),
                enabled=bool(r.get("enabled", True)),
                source=_parse_expression(r.get("source"), r.get("source_regex")),
                medium=_parse_expression(r.get("medium"), r.get("medium_regex")),
                campaign=_parse_expression(r.get("campaign")),
            )
        )

    rules.sort(key=lambda rule: (rule.priority, rule.name))

    return Taxonomy(
        channel_rules=rules,
        source_aliases=raw.get("source_aliases", {}),
        medium_aliases=raw.get("medium_aliases", {}),
    )


def save_taxonomy(taxonomy: Taxonomy) -> None:
    path = _taxonomy_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    def _serialize_expression(expr: MatchExpression) -> Dict[str, str]:
        return {
            "operator": expr.normalize_operator(),
            "value": expr.value or "",
        }

    payload = {
        "channel_rules": [
            {
                "name": r.name,
                "channel": r.channel,
                "priority": r.priority,
                "enabled": r.enabled,
                "source": _serialize_expression(r.source),
                "medium": _serialize_expression(r.medium),
                "campaign": _serialize_expression(r.campaign),
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
    raw_campaign = str(tp.get("utm_campaign") or tp.get("campaign") or "").lower()

    source = taxonomy.source_aliases.get(raw_source, raw_source)
    medium = taxonomy.medium_aliases.get(raw_medium, raw_medium)

    # channel mapping
    channel = str(tp.get("channel") or "")
    for rule in sorted(taxonomy.channel_rules, key=lambda r: (r.priority, r.name)):
        if rule.matches(source, medium, raw_campaign):
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

