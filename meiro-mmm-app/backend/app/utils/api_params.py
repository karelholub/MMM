"""Helpers for resilient API query parameter parsing."""

from __future__ import annotations

from typing import Optional


def clamp_int(value: Optional[int], *, default: int, minimum: int, maximum: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except Exception:
        return default
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def resolve_per_page(
    *,
    per_page: Optional[int] = None,
    page_size: Optional[int] = None,
    limit: Optional[int] = None,
    default: int = 20,
    maximum: int = 100,
) -> int:
    candidate = per_page
    if candidate is None:
        candidate = page_size
    if candidate is None:
        candidate = limit
    return clamp_int(candidate, default=default, minimum=1, maximum=maximum)


def resolve_sort_dir(*, sort: Optional[str] = None, order: Optional[str] = None, default: str = "desc") -> str:
    options = [sort, order]
    for value in options:
        token = str(value or "").strip().lower()
        if token in {"asc", "desc"}:
            return token
    return default
