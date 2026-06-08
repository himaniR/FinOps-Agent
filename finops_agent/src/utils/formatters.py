```python
"""Formatting helpers for currency, dates, and markdown tables."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


def fmt_usd(amount: float | int | None, decimals: int = 2) -> str:
    """Format a number as USD currency. None -> '$0.00'."""
    if amount is None:
        return "$0.00"
    return f"${amount:,.{decimals}f}"


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    """Format a number as a percentage. 0.13 -> '13.0%'."""
    if value is None:
        return "N/A"
    return f"{value * 100:.{decimals}f}%"


def fmt_pct_change(old: float | None, new: float | None, decimals: int = 1) -> str:
    """Format the percent change from old to new."""
    if old is None or new is None or old == 0:
        return "N/A"
    delta = (new - old) / abs(old)
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta * 100:.{decimals}f}%"


def fmt_dbu(dbus: float | None, decimals: int = 1) -> str:
    """Format DBU count."""
    if dbus is None:
        return "0"
    if dbus >= 1000:
        return f"{dbus / 1000:.{decimals}f}K DBU"
    return f"{dbus:.{decimals}f} DBU"


def fmt_bytes(num_bytes: int | float | None) -> str:
    """Format bytes as human-readable."""
    if num_bytes is None:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} EB"


def fmt_duration(seconds: float | int | None) -> str:
    """Format seconds as human-readable duration."""
    if seconds is None or seconds == 0:
        return "0s"
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs and not days:
        parts.append(f"{secs}s")
    return " ".join(parts) or "0s"


def utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def days_ago(n: int) -> datetime:
    """Return UTC datetime N days ago."""
    return utc_now() - timedelta(days=n)


def to_markdown_table(rows: Iterable[dict], columns: list[str] | None = None) -> str:
    """Convert a list of dicts to a markdown table.

    Args:
        rows: Iterable of dicts
        columns: Optional column order; if None, uses keys of first row
    """
    rows = list(rows)
    if not rows:
        return "_(no data)_"

    if columns is None:
        columns = list(rows[0].keys())

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    body_lines = []
    for r in rows:
        cells = [str(r.get(c, "")).replace("|", "\\|") for c in columns]
        body_lines.append("| " + " | ".join(cells) + " |")

    return "\n".join([header, sep, *body_lines])


def truncate(text: str, max_len: int = 100, suffix: str = "...") -> str:
    """Truncate a string to max_len, adding suffix if cut."""
    if len(text) <= max_len:
        return text
    return text[: max_len - len(suffix)] + suffix