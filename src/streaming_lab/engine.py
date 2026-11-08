"""One durable SQLite transaction for checkpoint, state, dedupe, and local sink.

This teaches idempotent local processing of an at-least-once source. It does NOT
make a cross-system exactly-once claim. Each engine follows ONE ordered source
partition with contiguous offsets. A real multi-partition topology needs a
checkpoint and progress policy per partition.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from .events import Event, InvalidEvent, epoch_ms, iso_time, window_start_ms


class OffsetGapError(ValueError):
    pass


class StreamFinalizedError(RuntimeError):
    pass


class InjectedCrash(RuntimeError):
    def __init__(self, stage: str, offset: int):
        self.stage, self.offset = stage, offset
        super().__init__(f"injected crash {stage} at offset {offset}")


STAT_NAMES = (
    "received", "accepted", "accepted_orders", "accepted_reviews", "duplicates", "late", "invalid",
    "revenue_windows_emitted", "review_windows_emitted", "alerts_emitted",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    checkpoint INTEGER NOT NULL DEFAULT -1,
    max_event_time_ms INTEGER,
    watermark_ms INTEGER,
    finalized INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS seen_events (
    event_id TEXT PRIMARY KEY, source_offset INTEGER NOT NULL,
    kind TEXT NOT NULL, disposition TEXT NOT NULL, payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_state (
    window_start_ms INTEGER PRIMARY KEY, orders INTEGER NOT NULL,
    revenue_cents INTEGER NOT NULL CHECK(typeof(revenue_cents)='integer')
);
CREATE TABLE IF NOT EXISTS review_state (
    window_start_ms INTEGER NOT NULL, product_id INTEGER NOT NULL,
    reviews INTEGER NOT NULL, negative_reviews INTEGER NOT NULL,
    PRIMARY KEY(window_start_ms, product_id)
);
CREATE TABLE IF NOT EXISTS realtime_revenue (
    window_start_ms INTEGER PRIMARY KEY, window_end_ms INTEGER NOT NULL,
    orders INTEGER NOT NULL, revenue_cents INTEGER NOT NULL CHECK(typeof(revenue_cents)='integer'),
    emitted_at_watermark_ms INTEGER, closure_reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS review_windows (
    window_start_ms INTEGER NOT NULL, window_end_ms INTEGER NOT NULL,
    product_id INTEGER NOT NULL, reviews INTEGER NOT NULL, negative_reviews INTEGER NOT NULL,
    emitted_at_watermark_ms INTEGER, closure_reason TEXT NOT NULL,
    PRIMARY KEY(window_start_ms, product_id)
);
CREATE TABLE IF NOT EXISTS review_alerts (
    product_id INTEGER NOT NULL, window_start_ms INTEGER NOT NULL,
    negative_reviews INTEGER NOT NULL, trigger_event_id TEXT NOT NULL,
    emitted_at_watermark_ms INTEGER,
    PRIMARY KEY(product_id, window_start_ms)
);
CREATE TABLE IF NOT EXISTS late_events (
    event_id TEXT PRIMARY KEY, source_offset INTEGER NOT NULL, kind TEXT NOT NULL,
    event_time_ms INTEGER NOT NULL, window_start_ms INTEGER NOT NULL,
    watermark_ms INTEGER NOT NULL, amount_cents INTEGER,
    payload_json TEXT NOT NULL, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letters (
    source_offset INTEGER PRIMARY KEY, payload_json TEXT NOT NULL, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accepted_reviews (
    event_id TEXT PRIMARY KEY, product_id INTEGER NOT NULL,
    event_time_ms INTEGER NOT NULL, rating INTEGER NOT NULL, text TEXT NOT NULL
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


class StreamEngine:
    """A small, inspectable stateful stream processor.

    Watermark = max(previous watermark, maximum accepted unique event time minus
    out_of_order_seconds). A window [start, end) is closed when watermark >=
    end + grace_seconds. Wall clock / polling never advance event-time progress.
    Deduplication history is durable and intentionally unbounded for this lab.
    """

    def __init__(self, db_path: Path | str, *, window_seconds: int = 60, grace_seconds: int = 10,
                 out_of_order_seconds: int = 0, alert_threshold: int = 3):
        config = {"window_seconds": window_seconds, "grace_seconds": grace_seconds,
                  "out_of_order_seconds": out_of_order_seconds, "alert_threshold": alert_threshold}
        for key, value in config.items():
            minimum = 1 if key in ("window_seconds", "alert_threshold") else 0
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{key} must be an integer >= {minimum}")
        self.path = str(db_path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.config = config
        self.window_ms, self.grace_ms, self.delay_ms = window_seconds * 1000, grace_seconds * 1000, out_of_order_seconds * 1000
        self.con = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=FULL")
        self.con.execute("PRAGMA foreign_keys=ON")
        self.con.executescript(SCHEMA)
        self.con.execute("BEGIN IMMEDIATE")
        try:
            row = self.con.execute("SELECT * FROM stream_meta WHERE singleton=1").fetchone()
            if row is None:
                self.con.execute("INSERT INTO stream_meta(singleton,schema_version,config_json) VALUES(1,1,?)", (_json(config),))
                self.con.executemany("INSERT INTO counters(name,value) VALUES(?,0)", [(key,) for key in STAT_NAMES])
            elif row["schema_version"] != 1 or json.loads(row["config_json"]) != config:
                raise ValueError("This DB uses different settings/schema; reopen with its original configuration or use a fresh run directory.")
            self.con.execute("COMMIT")
        except BaseException:
            if self.con.in_transaction:
                self.con.execute("ROLLBACK")
            self.con.close()
            raise

    def __enter__(self) -> StreamEngine:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.con.close()

    @property
    def checkpoint(self) -> int:
        return self.con.execute("SELECT checkpoint FROM stream_meta WHERE singleton=1").fetchone()[0]

    @property
    def next_offset(self) -> int:
        return self.checkpoint + 1

    def _bump(self, name: str, value: int = 1) -> None:
        self.con.execute("UPDATE counters SET value=value+? WHERE name=?", (value, name))

    def ingest(self, event: Event | Mapping[str, Any], *, offset: int,
               crash_at: str | None = None) -> dict[str, Any]:
        """Atomically consume the next offset, or ignore an already committed one.

        before_commit: rollback every change, so a replay processes the record.
        after_commit: durable change survives; replay ignores this old offset.
        Invalid/duplicate/late records STILL advance the checkpoint. Offset gaps
        raise rather than silently losing source records.
        """
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        if crash_at not in (None, "before_commit", "after_commit"):
            raise ValueError("crash_at must be before_commit or after_commit")
        self.con.execute("BEGIN IMMEDIATE")
        try:
            meta = self.con.execute("SELECT * FROM stream_meta WHERE singleton=1").fetchone()
            if offset <= meta["checkpoint"]:
                self.con.execute("COMMIT")
                return {"status": "replayed", "offset": offset, "checkpoint": meta["checkpoint"]}
            if meta["finalized"]:
                raise StreamFinalizedError("This finite demo was finalized. Start a fresh run for new input.")
            if offset != meta["checkpoint"] + 1:
                raise OffsetGapError(f"expected offset {meta['checkpoint'] + 1}, received {offset}")
            self._bump("received")
            try:
                parsed = event if isinstance(event, Event) else Event.from_dict(event)
                event_ms = epoch_ms(parsed.event_time)
            except (InvalidEvent, ValueError, TypeError, OverflowError) as exc:
                try:
                    raw = _json(event)
                except (ValueError, TypeError):
                    raw = _json({"unserializable_payload": repr(event)})
                self.con.execute("INSERT INTO dead_letters VALUES(?,?,?)", (offset, raw, str(exc)))
                self._bump("invalid")
                result = {"status": "invalid", "offset": offset, "reason": str(exc)}
            else:
                result = self._process_valid(parsed, event_ms, offset, meta)
            self.con.execute("UPDATE stream_meta SET checkpoint=? WHERE singleton=1", (offset,))
            if crash_at == "before_commit":
                raise InjectedCrash(crash_at, offset)
            self.con.execute("COMMIT")
        except BaseException:
            if self.con.in_transaction:
                self.con.execute("ROLLBACK")
            raise
        if crash_at == "after_commit":
            raise InjectedCrash(crash_at, offset)
        result["checkpoint"] = offset
        return result

    def _process_valid(self, event: Event, event_ms: int, offset: int, meta: sqlite3.Row) -> dict[str, Any]:
        if self.con.execute("SELECT 1 FROM seen_events WHERE event_id=?", (event.event_id,)).fetchone():
            self._bump("duplicates")
            return {"status": "duplicate", "offset": offset, "event_id": event.event_id}
        start = window_start_ms(event_ms, self.config["window_seconds"])
        end = start + self.window_ms
        previous_wm = meta["watermark_ms"]
        late = previous_wm is not None and previous_wm >= end + self.grace_ms
        payload = _json(event.to_dict())
        self.con.execute("INSERT INTO seen_events VALUES(?,?,?,?,?)",
                         (event.event_id, offset, event.kind, "late" if late else "accepted", payload))
        if late:
            self.con.execute("INSERT INTO late_events VALUES(?,?,?,?,?,?,?,?,?)",
                             (event.event_id, offset, event.kind, event_ms, start, previous_wm, event.amount_cents,
                              payload, "watermark >= window_end + grace"))
            self._bump("late")
            return {"status": "late", "offset": offset, "event_id": event.event_id,
                    "watermark_ms": previous_wm, "window_start": iso_time(start)}
        self._bump("accepted")
        self._bump("accepted_orders" if event.kind == "order" else "accepted_reviews")
        maximum = max(event_ms, meta["max_event_time_ms"]) if meta["max_event_time_ms"] is not None else event_ms
        watermark = max(previous_wm, maximum - self.delay_ms) if previous_wm is not None else maximum - self.delay_ms
        self.con.execute("UPDATE stream_meta SET max_event_time_ms=?,watermark_ms=? WHERE singleton=1", (maximum, watermark))
        if event.kind == "order":
            self.con.execute("""INSERT INTO order_state VALUES(?,1,?) ON CONFLICT(window_start_ms)
                DO UPDATE SET orders=orders+1,revenue_cents=revenue_cents+excluded.revenue_cents""", (start, event.amount_cents))
        else:
            self.con.execute("INSERT INTO accepted_reviews VALUES(?,?,?,?,?)",
                             (event.event_id, event.product_id, event_ms, event.rating, event.text))
            self.con.execute("""INSERT INTO review_state VALUES(?,?,1,?) ON CONFLICT(window_start_ms,product_id)
                DO UPDATE SET reviews=reviews+1,negative_reviews=negative_reviews+excluded.negative_reviews""",
                             (start, event.product_id, int(event.rating <= 2)))
            count = self.con.execute("SELECT negative_reviews FROM review_state WHERE window_start_ms=? AND product_id=?",
                                     (start, event.product_id)).fetchone()[0]
            if count >= self.config["alert_threshold"]:
                changed = self.con.execute("INSERT OR IGNORE INTO review_alerts VALUES(?,?,?,?,?)",
                                           (event.product_id, start, count, event.event_id, watermark)).rowcount
                if changed:
                    self._bump("alerts_emitted")
        closed = self._close_windows(watermark, "watermark")
        return {"status": "accepted", "offset": offset, "event_id": event.event_id,
                "watermark_ms": watermark, "window_start": iso_time(start), "closed_windows": closed}

    def _close_windows(self, watermark: int | None, reason: str, all_windows: bool = False) -> int:
        if watermark is None and not all_windows:
            return 0
        cutoff = watermark - self.window_ms - self.grace_ms if watermark is not None else 0
        predicate, args = ("", ()) if all_windows else (" WHERE window_start_ms<=?", (cutoff,))
        orders = self.con.execute("SELECT * FROM order_state" + predicate + " ORDER BY window_start_ms", args).fetchall()
        reviews = self.con.execute("SELECT * FROM review_state" + predicate + " ORDER BY window_start_ms,product_id", args).fetchall()
        for row in orders:
            self.con.execute("INSERT INTO realtime_revenue VALUES(?,?,?,?,?,?)",
                             (row["window_start_ms"], row["window_start_ms"] + self.window_ms, row["orders"], row["revenue_cents"], watermark, reason))
            self.con.execute("DELETE FROM order_state WHERE window_start_ms=?", (row["window_start_ms"],))
        for row in reviews:
            self.con.execute("INSERT INTO review_windows VALUES(?,?,?,?,?,?,?)",
                             (row["window_start_ms"], row["window_start_ms"] + self.window_ms, row["product_id"], row["reviews"], row["negative_reviews"], watermark, reason))
            self.con.execute("DELETE FROM review_state WHERE window_start_ms=? AND product_id=?", (row["window_start_ms"], row["product_id"]))
        self._bump("revenue_windows_emitted", len(orders))
        self._bump("review_windows_emitted", len(reviews))
        return len(orders) + len(reviews)

    def finalize_demo(self) -> dict[str, Any]:
        """Explicit finite end-of-input only. The watermark does not change.

        Idleness is not end-of-input. A live run stays open unless the caller
        deliberately invokes this method (or run_live(..., finalize=True)).
        """
        self.con.execute("BEGIN IMMEDIATE")
        try:
            meta = self.con.execute("SELECT * FROM stream_meta WHERE singleton=1").fetchone()
            if not meta["finalized"]:
                self._close_windows(meta["watermark_ms"], "demo_end_of_input", all_windows=True)
                self.con.execute("UPDATE stream_meta SET finalized=1 WHERE singleton=1")
            self.con.execute("COMMIT")
        except BaseException:
            if self.con.in_transaction:
                self.con.execute("ROLLBACK")
            raise
        return self.snapshot()

    def search_reviews(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        """Lexical substring search in accepted reviews; not vector/semantic AI."""
        if not isinstance(query, str) or not query.strip():
            return []
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 to 100")
        return [self._row(row) for row in self.con.execute(
            "SELECT * FROM accepted_reviews WHERE instr(lower(text),lower(?))>0 ORDER BY event_time_ms,event_id LIMIT ?", (query, limit))]

    def _row(self, row: sqlite3.Row) -> dict[str, Any]:
        output = dict(row)
        for name in ("window_start_ms", "window_end_ms", "event_time_ms", "watermark_ms", "emitted_at_watermark_ms"):
            if name in output:
                output[name.removesuffix("_ms")] = iso_time(output[name])
        if "window_start_ms" in output and "window_end_ms" not in output:
            output["window_end_ms"] = output["window_start_ms"] + self.window_ms
            output["window_end"] = iso_time(output["window_end_ms"])
        if "payload_json" in output:
            output["payload"] = json.loads(output.pop("payload_json"))
        return output

    def snapshot(self) -> dict[str, Any]:
        """One consistent read snapshot. Counts include open and emitted windows."""
        self.con.execute("BEGIN")
        try:
            meta = self.con.execute("SELECT * FROM stream_meta WHERE singleton=1").fetchone()
            stats = dict(self.con.execute("SELECT name,value FROM counters"))
            def rows(table: str, order: str) -> list[dict[str, Any]]:
                return [self._row(row) for row in self.con.execute(f"SELECT * FROM {table} ORDER BY {order}")]
            open_orders = rows("order_state", "window_start_ms")
            open_reviews = rows("review_state", "window_start_ms,product_id")
            revenue = rows("realtime_revenue", "window_start_ms")
            reviews = rows("review_windows", "window_start_ms,product_id")
            late = rows("late_events", "source_offset")
            output = {
                "checkpoint": meta["checkpoint"], "next_offset": meta["checkpoint"] + 1,
                "max_event_time_ms": meta["max_event_time_ms"], "max_event_time": iso_time(meta["max_event_time_ms"]),
                "watermark_ms": meta["watermark_ms"],
                "watermark": iso_time(meta["watermark_ms"]), "finalized": bool(meta["finalized"]),
                "config": dict(self.config), "stats": stats,
                "open_windows": open_orders, "open_review_windows": open_reviews,
                "revenue_windows": revenue, "review_windows": reviews,
                "alerts": rows("review_alerts", "window_start_ms,product_id"),
                "late_events": late, "dead_letters": rows("dead_letters", "source_offset"),
                "totals": {
                    "orders": sum(row["orders"] for row in open_orders + revenue),
                    "revenue_cents": sum(row["revenue_cents"] for row in open_orders + revenue),
                    "reviews": sum(row["reviews"] for row in open_reviews + reviews),
                    "negative_reviews": sum(row["negative_reviews"] for row in open_reviews + reviews),
                    "late_orders": sum(row["kind"] == "order" for row in late),
                    "late_order_cents": sum(row["amount_cents"] or 0 for row in late if row["kind"] == "order"),
                    "late_reviews": sum(row["kind"] == "review" for row in late),
                },
            }
            self.con.execute("COMMIT")
            return output
        except BaseException:
            if self.con.in_transaction:
                self.con.execute("ROLLBACK")
            raise
