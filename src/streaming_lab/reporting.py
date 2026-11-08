"""Small presentation helpers; no pandas dependency and no currency floats."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
from typing import Any


def money(cents: int, currency: str = "₪") -> str:
    if isinstance(cents, bool) or not isinstance(cents, int):
        raise TypeError("money() expects integer cents")
    sign = "-" if cents < 0 else ""
    whole, fraction = divmod(abs(cents), 100)
    return f"{sign}{currency}{whole:,}.{fraction:02d}"


def fresh_run_directory(base: str | Path = "lab_runs", *, prefix: str = "run-") -> Path:
    """Create a new unique directory; never unlink a prior run or user file."""
    base = Path(base)
    base.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-")
    return Path(tempfile.mkdtemp(prefix=prefix + timestamp, dir=base))


def format_summary(snapshot: dict[str, Any], *, truth: dict[str, int] | None = None) -> str:
    stats, totals = snapshot["stats"], snapshot["totals"]
    output = [
        f"checkpoint={snapshot['checkpoint']} | watermark={snapshot['watermark']} | finalized={snapshot['finalized']}",
        f"received={stats['received']} | accepted={stats['accepted']} | duplicates={stats['duplicates']} | late={stats['late']} | invalid={stats['invalid']}",
        f"orders={totals['orders']} | revenue={money(totals['revenue_cents'])} ({totals['revenue_cents']} cents)",
        f"reviews={totals['reviews']} | negative reviews={totals['negative_reviews']} | alerts={len(snapshot['alerts'])}",
        f"open order windows={len(snapshot['open_windows'])} | emitted order windows={len(snapshot['revenue_windows'])}",
        f"late side output: {totals['late_orders']} orders, {money(totals['late_order_cents'])}; {totals['late_reviews']} reviews",
    ]
    if truth is not None:
        output.extend([
            f"batch truth: {truth['orders']} orders, {money(truth['revenue_cents'])}",
            f"reconciliation: accepted {totals['revenue_cents']} + late {totals['late_order_cents']} = {totals['revenue_cents'] + totals['late_order_cents']} cents",
        ])
    return "\n".join(output)


def canonical_results(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Deterministic state/sink projection used to compare recovery with baseline."""
    return {name: snapshot[name] for name in (
        "checkpoint", "watermark", "watermark_ms", "max_event_time_ms", "finalized", "stats", "open_windows",
        "open_review_windows", "revenue_windows", "review_windows", "alerts", "late_events", "dead_letters", "totals", "config",
    )}
