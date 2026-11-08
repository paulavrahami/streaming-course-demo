"""Contract tests for the teaching stream, including real process termination.

Run from the repository root: uv run python -m unittest discover -s tests -v
Only stdlib and temporary directories are used. No broker or network is needed.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from streaming_lab import (
    Delivery, Event, InjectedCrash, InvalidEvent, OffsetGapError,
    StreamEngine, StreamFinalizedError, batch_truth, generate_order_fixture,
    generate_review_fixture, run_live, run_microbatch, teaching_fixture,
)
from streaming_lab.events import EPOCH, T0, epoch_ms, window_start_ms
from streaming_lab.reporting import canonical_results, fresh_run_directory, money


def order(event_id: str, second: float, cents: int = 100) -> Event:
    return Event(event_id, "order", T0 + timedelta(seconds=second), 5, amount_cents=cents)


def review(event_id: str, second: float, product: int = 5, rating: int = 1,
           text: str = "לא עובד") -> Event:
    return Event(event_id, "review", T0 + timedelta(seconds=second), product,
                 rating=rating, text=text)


def consume(engine: StreamEngine, deliveries: list[Delivery]) -> list[str]:
    return [engine.ingest(item.event, offset=item.offset)["status"] for item in deliveries]


class EventTests(unittest.TestCase):
    def test_timezone_normalization_and_round_trip(self):
        event = Event("x", "order", "2026-11-01T12:00:00+02:00", 1, amount_cents=1990)
        self.assertEqual(event.event_time, T0)
        self.assertEqual(Event.from_dict(event.to_dict()), event)

    def test_money_rejects_float_bool_negative_and_missing(self):
        for value in (19.90, True, -1, None, "1990"):
            with self.subTest(value=value), self.assertRaises(InvalidEvent):
                order("x", 0, value)
        self.assertEqual(order("free", 0, 0).amount_cents, 0)

    def test_invalid_identity_rating_and_timezone(self):
        raw = order("x", 0).to_dict()
        for change in ({"event_id": " "}, {"kind": "refund"}, {"product_id": True},
                       {"product_id": 0}, {"event_time": "2026-11-01T10:00:00"},
                       {"event_time": "nonsense"}, {"rating": 1}):
            with self.subTest(change=change), self.assertRaises(InvalidEvent):
                Event.from_dict(raw | change)
        for rating in (0, 6, False, 2.0):
            with self.subTest(rating=rating), self.assertRaises(InvalidEvent):
                review("r", 0, rating=rating)

    def test_millisecond_boundary_and_negative_epoch(self):
        before = T0 + timedelta(seconds=60, microseconds=-1)
        self.assertEqual(window_start_ms(before), epoch_ms(T0))
        self.assertEqual(window_start_ms(T0 + timedelta(seconds=60)), epoch_ms(T0) + 60_000)
        self.assertEqual(epoch_ms(EPOCH - timedelta(microseconds=1)), -1)
        self.assertEqual(window_start_ms(EPOCH - timedelta(microseconds=1)), -60_000)

    def test_fixtures_are_deterministic_and_keep_source_offsets(self):
        first, again = generate_order_fixture(), generate_order_fixture()
        self.assertEqual(first, again)
        self.assertEqual([x.offset for x in first], list(range(len(first))))
        self.assertEqual(batch_truth(first)["orders"], 300)
        self.assertEqual(len({x.event.event_id for x in first}), 300)
        self.assertGreater(len(first), 300)
        self.assertTrue(all(isinstance(x.event.amount_cents, int) for x in first))
        self.assertEqual(generate_order_fixture(0), [])

    def test_batch_truth_counts_valid_identity_once(self):
        truth = batch_truth(teaching_fixture())
        self.assertEqual(truth, {"unique_events": 14, "orders": 9, "revenue_cents": 13_500,
                                 "reviews": 5, "negative_reviews": 4, "invalid": 1})
        self.assertEqual(batch_truth(generate_review_fixture())["reviews"], 5)

    def test_money_rendering_keeps_exact_cents(self):
        self.assertEqual(money(19), "₪0.19")
        self.assertEqual(money(13_500), "₪135.00")
        self.assertEqual(money(-1), "-₪0.01")
        with self.assertRaises(TypeError):
            money(0.19)


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite"
        self.engine = StreamEngine(self.path)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def reopen(self, **config):
        self.engine.close()
        self.engine = StreamEngine(self.path, **config)

    def test_half_open_windows_separate_exact_boundary(self):
        consume(self.engine, [Delivery(0, order("before", 59.999999, 101)),
                              Delivery(1, order("boundary", 60, 202))])
        windows = self.engine.snapshot()["open_windows"]
        self.assertEqual([(x["window_start_ms"], x["revenue_cents"]) for x in windows],
                         [(epoch_ms(T0), 101), (epoch_ms(T0) + 60_000, 202)])

    def test_grace_equality_closes_and_preserves_late_record(self):
        self.engine.ingest(order("first", 5), offset=0)
        self.engine.ingest(order("advance", 69.999), offset=1)
        self.assertEqual(len(self.engine.snapshot()["revenue_windows"]), 0)
        self.engine.ingest(order("grace-in", 20, 333), offset=2)
        self.engine.ingest(order("edge", 70), offset=3)
        closed = self.engine.snapshot()["revenue_windows"]
        self.assertEqual(len(closed), 1)
        self.assertEqual((closed[0]["orders"], closed[0]["revenue_cents"]), (2, 433))
        self.assertEqual(closed[0]["closure_reason"], "watermark")
        result = self.engine.ingest(order("too-late", 20, 444), offset=4)
        self.assertEqual(result["status"], "late")
        snap = self.engine.snapshot()
        self.assertEqual(snap["late_events"][0]["payload"]["amount_cents"], 444)
        self.assertEqual(snap["totals"]["late_order_cents"], 444)
        self.assertEqual(snap["checkpoint"], 4)
        self.assertEqual(snap["revenue_windows"], closed)

    def test_watermark_lag_and_monotonicity_are_explicit(self):
        self.engine.close()
        self.path = Path(self.tmp.name) / "lag.sqlite"
        self.engine = StreamEngine(self.path, out_of_order_seconds=20, grace_seconds=10)
        self.engine.ingest(order("newer", 89), offset=0)
        first = self.engine.snapshot()
        self.assertEqual(first["max_event_time_ms"], epoch_ms(T0) + 89_000)
        self.assertEqual(first["watermark_ms"], epoch_ms(T0) + 69_000)
        self.assertEqual(self.engine.ingest(order("older", 5), offset=1)["status"], "accepted")
        self.assertEqual(self.engine.snapshot()["watermark_ms"], first["watermark_ms"])
        self.engine.ingest(order("newest", 90), offset=2)
        self.assertEqual(self.engine.snapshot()["watermark_ms"], epoch_ms(T0) + 70_000)
        self.assertEqual(self.engine.ingest(order("oldest", 6), offset=3)["status"], "late")

    def test_duplicate_future_timestamp_cannot_advance_watermark(self):
        self.engine.ingest(order("same-id", 5), offset=0)
        old = self.engine.snapshot()["watermark_ms"]
        result = self.engine.ingest(order("same-id", 3600, 9999), offset=1)
        self.assertEqual(result["status"], "duplicate")
        self.assertEqual(self.engine.snapshot()["watermark_ms"], old)
        self.assertEqual(self.engine.snapshot()["totals"]["revenue_cents"], 100)

    def test_invalid_future_timestamp_does_not_advance_watermark(self):
        self.engine.ingest(order("ok", 5), offset=0)
        old = self.engine.snapshot()["watermark_ms"]
        invalid = order("bad", 3600).to_dict() | {"amount_cents": 19.9}
        result = self.engine.ingest(invalid, offset=1)
        self.assertEqual(result["status"], "invalid")
        snap = self.engine.snapshot()
        self.assertEqual(snap["watermark_ms"], old)
        self.assertEqual(snap["checkpoint"], 1)
        self.assertEqual(snap["dead_letters"][0]["payload"]["amount_cents"], 19.9)

    def test_malformed_payload_is_durably_quarantined(self):
        for offset, payload in enumerate((None, [], {"event_id": "missing"},
                                           {"event_id": "nan", "amount_cents": float("nan")})):
            self.assertEqual(self.engine.ingest(payload, offset=offset)["status"], "invalid")
        self.reopen()
        snap = self.engine.snapshot()
        self.assertEqual(snap["checkpoint"], 3)
        self.assertEqual(snap["stats"]["invalid"], 4)
        self.assertEqual(len(snap["dead_letters"]), 4)
        self.assertIsNone(snap["watermark_ms"])

    def test_late_retry_is_deduplicated_and_checkpoint_advances(self):
        consume(self.engine, [Delivery(0, order("ahead", 100)),
                              Delivery(1, order("late", 5)), Delivery(2, order("late", 5))])
        self.reopen()
        snap = self.engine.snapshot()
        self.assertEqual((snap["stats"]["late"], snap["stats"]["duplicates"]), (1, 1))
        self.assertEqual(len(snap["late_events"]), 1)
        self.assertEqual(snap["checkpoint"], 2)

    def test_offset_replay_is_no_op_and_gap_rolls_back(self):
        event = order("a", 5)
        self.engine.ingest(event, offset=0)
        before = self.engine.snapshot()
        self.assertEqual(self.engine.ingest(event, offset=0)["status"], "replayed")
        self.assertEqual(self.engine.snapshot(), before)
        with self.assertRaises(OffsetGapError):
            self.engine.ingest(order("gap", 100), offset=2)
        self.assertEqual(self.engine.snapshot(), before)
        self.engine.ingest(order("next", 6), offset=1)
        self.assertEqual(self.engine.next_offset, 2)

    def test_finalization_is_explicit_idempotent_and_does_not_move_watermark(self):
        self.engine.ingest(order("a", 5), offset=0)
        before = self.engine.snapshot()
        self.assertFalse(before["finalized"])
        self.assertEqual(len(before["open_windows"]), 1)
        for _ in range(3):
            self.assertEqual(self.engine.snapshot(), before)
        finalized = self.engine.finalize_demo()
        self.assertEqual(finalized["watermark_ms"], before["watermark_ms"])
        self.assertTrue(finalized["finalized"])
        self.assertEqual(finalized["open_windows"], [])
        self.assertEqual(finalized["revenue_windows"][0]["closure_reason"], "demo_end_of_input")
        self.assertEqual(self.engine.finalize_demo(), finalized)
        self.assertEqual(self.engine.ingest(order("a", 5), offset=0)["status"], "replayed")
        with self.assertRaises(StreamFinalizedError):
            self.engine.ingest(order("b", 10), offset=1)
        self.assertEqual(self.engine.snapshot(), finalized)

    def test_empty_finite_input_can_be_finalized(self):
        result = self.engine.finalize_demo()
        self.assertTrue(result["finalized"])
        self.assertIsNone(result["watermark_ms"])
        self.assertEqual(result["checkpoint"], -1)
        self.assertEqual(result["revenue_windows"], [])

    def test_review_alert_once_per_product_window_across_restart_and_retry(self):
        items = [review("r1", 5), review("r2", 10), review("r3", 15), review("r4", 20)]
        for offset, event in enumerate(items):
            self.engine.ingest(event, offset=offset)
        self.reopen()
        self.engine.ingest(items[2], offset=4)
        self.engine.ingest(review("r5", 25), offset=5)
        self.assertEqual(len(self.engine.snapshot()["alerts"]), 1)
        self.assertEqual(self.engine.snapshot()["alerts"][0]["negative_reviews"], 3)
        self.assertEqual(self.engine.snapshot()["alerts"][0]["trigger_event_id"], "r3")
        for second in (26, 27, 28):
            self.engine.ingest(review(f"p3-{second}", second, product=3), offset=self.engine.next_offset)
        for second in (65, 66, 67):
            self.engine.ingest(review(f"new-window-{second}", second), offset=self.engine.next_offset)
        self.assertEqual(len(self.engine.snapshot()["alerts"]), 3)
        self.assertEqual(self.engine.snapshot()["stats"]["alerts_emitted"], 3)

    def test_positive_reviews_and_late_reviews_do_not_trigger_alert(self):
        for offset, event in enumerate([review("p", 5, rating=5), review("one", 6),
                                         review("two", 7), order("advance", 100), review("late", 8)]):
            self.engine.ingest(event, offset=offset)
        snap = self.engine.snapshot()
        self.assertEqual(snap["alerts"], [])
        self.assertEqual(snap["totals"]["reviews"], 3)
        self.assertEqual(snap["totals"]["negative_reviews"], 2)
        self.assertEqual(snap["totals"]["late_reviews"], 1)

    def test_hebrew_lexical_search_and_sql_metacharacters(self):
        self.engine.ingest(review("broken", 5, text="המוצר הגיע שבור"), offset=0)
        self.engine.ingest(review("good", 6, rating=5, text="מוצר מעולה"), offset=1)
        self.assertEqual([row["event_id"] for row in self.engine.search_reviews("שבור")], ["broken"])
        self.assertEqual(self.engine.search_reviews("' OR 1=1 --"), [])
        self.assertEqual(self.engine.search_reviews(""), [])
        with self.assertRaises(ValueError):
            self.engine.search_reviews("מוצר", limit=0)

    def test_settings_are_persisted_and_cannot_change_mid_stream(self):
        self.engine.ingest(order("a", 5), offset=0)
        with self.assertRaises(ValueError):
            StreamEngine(self.path, grace_seconds=120)
        self.assertEqual(self.engine.snapshot()["totals"]["orders"], 1)
        self.reopen()
        self.assertEqual(self.engine.next_offset, 1)

    def test_teaching_fixture_reconciles_stream_plus_late_to_batch(self):
        statuses = consume(self.engine, teaching_fixture())
        snap = self.engine.finalize_demo()
        self.assertEqual(statuses, ["accepted", "accepted", "accepted", "accepted", "accepted",
                                    "duplicate", "accepted", "accepted", "accepted", "late",
                                    "accepted", "invalid", "accepted", "accepted", "duplicate", "late", "accepted"])
        self.assertEqual(snap["stats"], {"received": 17, "accepted": 12, "accepted_orders": 7,
                                       "accepted_reviews": 5, "duplicates": 2, "late": 2, "invalid": 1,
                                       "revenue_windows_emitted": 4, "review_windows_emitted": 2,
                                       "alerts_emitted": 1})
        self.assertEqual(snap["totals"]["revenue_cents"], 12_200)
        self.assertEqual([(row["orders"], row["revenue_cents"]) for row in snap["revenue_windows"]],
                         [(2, 2500), (2, 5000), (2, 3500), (1, 1200)])
        truth = batch_truth(teaching_fixture())
        self.assertEqual(snap["totals"]["revenue_cents"] + snap["totals"]["late_order_cents"], truth["revenue_cents"])
        self.assertEqual(snap["totals"]["orders"] + snap["totals"]["late_orders"], truth["orders"])
        self.assertEqual(snap["checkpoint"], 16)

    def test_generator_reconciles_for_different_grace_settings(self):
        fixture = generate_order_fixture()
        truth = batch_truth(fixture)
        late_counts = []
        for grace in (0, 10, 120):
            with self.subTest(grace=grace), StreamEngine(Path(self.tmp.name) / f"grace-{grace}.sqlite", grace_seconds=grace) as engine:
                consume(engine, fixture)
                snap = engine.finalize_demo()
                self.assertEqual(snap["totals"]["orders"] + snap["totals"]["late_orders"], 300)
                self.assertEqual(snap["totals"]["revenue_cents"] + snap["totals"]["late_order_cents"], truth["revenue_cents"])
                self.assertEqual(snap["stats"]["duplicates"], len(fixture) - 300)
                self.assertEqual(snap["stats"]["invalid"], 0)
                self.assertEqual(snap["checkpoint"], len(fixture) - 1)
                late_counts.append(snap["stats"]["late"])
        self.assertEqual(late_counts, sorted(late_counts, reverse=True))
        self.assertEqual(late_counts[-1], 0)

    def test_input_validation_does_not_start_a_transaction(self):
        for offset in (-1, True, 1.5):
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                self.engine.ingest(order("a", 0), offset=offset)
        with self.assertRaises(ValueError):
            self.engine.ingest(order("a", 0), offset=0, crash_at="during-unrelated-system")
        self.assertEqual(self.engine.checkpoint, -1)
        self.assertFalse(self.engine.con.in_transaction)

    def test_fresh_run_directory_preserves_existing_files(self):
        first = fresh_run_directory(self.tmp.name)
        marker = first / "keep.txt"
        marker.write_text("student work", encoding="utf-8")
        second = fresh_run_directory(self.tmp.name)
        self.assertNotEqual(first, second)
        self.assertEqual(marker.read_text(encoding="utf-8"), "student work")


class RecoveryTests(unittest.TestCase):
    def test_crash_at_each_offset_before_and_after_commit_matches_uninterrupted_result(self):
        fixture = teaching_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            with StreamEngine(Path(tmp) / "baseline.sqlite") as baseline:
                consume(baseline, fixture)
                expected = canonical_results(baseline.finalize_demo())
            for stage in ("before_commit", "after_commit"):
                for crash_offset in range(len(fixture)):
                    with self.subTest(stage=stage, offset=crash_offset):
                        path = Path(tmp) / f"{stage}-{crash_offset}.sqlite"
                        with StreamEngine(path) as engine:
                            consume(engine, fixture[:crash_offset])
                            with self.assertRaises(InjectedCrash):
                                engine.ingest(fixture[crash_offset].event, offset=crash_offset, crash_at=stage)
                        with StreamEngine(path) as restarted:
                            self.assertEqual(restarted.checkpoint, crash_offset - (stage == "before_commit"))
                            consume(restarted, fixture)
                            self.assertEqual(canonical_results(restarted.finalize_demo()), expected)
                            self.assertEqual(consume(restarted, fixture), ["replayed"] * len(fixture))
                            self.assertEqual(canonical_results(restarted.snapshot()), expected)

    def test_real_process_exit_leaves_checkpoint_sink_and_state_in_agreement(self):
        """os._exit skips rollback/close/finally; SQLite must recover its WAL."""
        script = r'''
import os
from pathlib import Path
import sys
import streaming_lab.engine as engine_module
from streaming_lab import StreamEngine, teaching_fixture
path, stage = sys.argv[1:]
engine = StreamEngine(Path(path))
items = teaching_fixture()
for item in items[:8]:
    engine.ingest(item.event, offset=item.offset)
def exit_without_cleanup(stage, offset):
    os._exit(42)
engine_module.InjectedCrash = exit_without_cleanup
engine.ingest(items[8].event, offset=8, crash_at=stage)
'''
        with tempfile.TemporaryDirectory() as tmp:
            for stage, checkpoint, closed_count in (("before_commit", 7, 0), ("after_commit", 8, 1)):
                with self.subTest(stage=stage):
                    path = Path(tmp) / f"hard-{stage}.sqlite"
                    proc = subprocess.run([sys.executable, "-c", script, str(path), stage],
                                          capture_output=True, text=True, timeout=20)
                    self.assertEqual(proc.returncode, 42, proc.stderr)
                    with StreamEngine(path) as restarted:
                        snap = restarted.snapshot()
                        self.assertEqual(snap["checkpoint"], checkpoint)
                        self.assertEqual(len(snap["revenue_windows"]), closed_count)
                        self.assertEqual(snap["stats"]["received"], checkpoint + 1)
                        consume(restarted, teaching_fixture())
                        final = restarted.finalize_demo()
                        self.assertEqual(final["totals"]["revenue_cents"], 12_200)
                        self.assertEqual(final["stats"]["alerts_emitted"], 1)
                        self.assertEqual(final["stats"]["received"], 17)


class LiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = StreamEngine(Path(self.tmp.name) / "live.sqlite")

    async def asyncTearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    async def test_finite_source_drains_bounded_queue_with_backpressure(self):
        observed = []
        result = await run_live(self.engine, teaching_fixture(), duration_s=None, interval_s=0,
                                processing_delay_s=0.002, queue_maxsize=2,
                                on_snapshot=lambda snapshot: observed.append(snapshot["checkpoint"]))
        self.assertEqual((result["produced"], result["consumed"]), (17, 17))
        self.assertEqual(result["max_queue_depth"], 2)
        self.assertGreater(result["backpressure_waits"], 0)
        self.assertGreater(result["producer_wait_s"], 0)
        self.assertEqual(result["stop_reason"], "end_of_input")
        self.assertEqual(observed, list(range(17)))
        self.assertFalse(result["snapshot"]["finalized"])
        self.assertEqual(len(result["snapshot"]["open_windows"]), 1)

    async def test_duration_stops_admission_then_drains_without_finalizing(self):
        result = await run_live(self.engine, duration_s=0.04, interval_s=0,
                                processing_delay_s=0.01, queue_maxsize=2)
        self.assertEqual(result["produced"], result["consumed"])
        self.assertGreater(result["produced"], 0)
        self.assertLessEqual(result["max_queue_depth"], 2)
        self.assertEqual(result["stop_reason"], "duration")
        self.assertFalse(result["snapshot"]["finalized"])
        self.assertEqual(result["snapshot"]["checkpoint"], result["consumed"] - 1)
        self.assertEqual(result["event_time_clock"], "synthetic_2s_per_delivery")

    async def test_callback_queue_metrics_show_backpressure_and_drain_without_persisting(self):
        observed = []
        result = await run_live(self.engine, teaching_fixture(), duration_s=None, interval_s=0,
                                processing_delay_s=0.002, queue_maxsize=2,
                                on_snapshot=lambda snapshot: observed.append(snapshot))
        metrics = [snapshot["queue"] for snapshot in observed]
        self.assertEqual([item["consumed"] for item in metrics], list(range(1, 18)))
        self.assertTrue(all(item["maxsize"] == 2 for item in metrics))
        self.assertTrue(all(0 <= item["depth"] <= 2 for item in metrics))
        self.assertTrue(all(item["depth"] == item["produced"] - item["consumed"] for item in metrics))
        self.assertIn(2, [item["depth"] for item in metrics])
        self.assertGreater(metrics[0]["backpressure_waits"], 0)
        self.assertEqual(metrics[-1]["depth"], 0)
        self.assertEqual((metrics[-1]["produced"], metrics[-1]["consumed"]), (17, 17))
        self.assertEqual(metrics[-1]["backpressure_waits"], result["backpressure_waits"])
        self.assertNotIn("queue", self.engine.snapshot())
        self.assertNotIn("queue", result["snapshot"])
        self.assertEqual(canonical_results(observed[-1]), canonical_results(result["snapshot"]))

    async def test_timed_source_callback_counters_never_consume_before_admission(self):
        observed = []
        result = await run_live(self.engine, duration_s=0.02, interval_s=0.001,
                                on_snapshot=lambda snapshot: observed.append(snapshot["queue"]))
        self.assertGreater(len(observed), 0)
        self.assertTrue(all(item["produced"] >= item["consumed"] for item in observed))
        self.assertTrue(all(0 <= item["depth"] <= item["maxsize"] for item in observed))
        self.assertEqual(observed[-1]["produced"], result["produced"])
        self.assertEqual(observed[-1]["consumed"], result["consumed"])

    async def test_synthetic_live_source_resumes_from_committed_offset(self):
        first = await run_live(self.engine, duration_s=0.025, interval_s=0.005)
        second = await run_live(self.engine, duration_s=0.025, interval_s=0.005)
        self.assertEqual(second["snapshot"]["checkpoint"] + 1, first["consumed"] + second["consumed"])
        self.assertEqual(second["snapshot"]["stats"]["received"], first["consumed"] + second["consumed"])

    async def test_async_callback_and_explicit_finalization(self):
        observed = []
        async def callback(snapshot):
            await asyncio.sleep(0)
            observed.append(snapshot["checkpoint"])
        result = await run_live(self.engine, [Delivery(0, order("single", 5))], duration_s=None,
                                interval_s=0, finalize=True, on_snapshot=callback)
        self.assertEqual(observed, [0])
        self.assertTrue(result["snapshot"]["finalized"])
        self.assertEqual(result["snapshot"]["watermark_ms"], epoch_ms(T0) + 5_000)
        self.assertEqual(result["snapshot"]["revenue_windows"][0]["closure_reason"], "demo_end_of_input")

    async def test_cancellation_cleans_up_tasks_and_keeps_committed_database(self):
        callback_started = asyncio.Event()
        async def pause_after_commit(snapshot):
            callback_started.set()
            await asyncio.sleep(10)
        task = asyncio.create_task(run_live(self.engine, duration_s=5, interval_s=0,
                                            queue_maxsize=1, on_snapshot=pause_after_commit))
        await asyncio.wait_for(callback_started.wait(), timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.engine.checkpoint, 0)
        running_names = {other.get_name() for other in asyncio.all_tasks() if not other.done()}
        self.assertNotIn("streaming-lab-producer", running_names)
        self.assertNotIn("streaming-lab-consumer", running_names)
        self.assertEqual(self.engine.snapshot()["totals"]["orders"], 1)

    async def test_invalid_queue_and_unbounded_duration_fail_fast(self):
        for changes in ({"queue_maxsize": 0}, {"queue_maxsize": True}, {"duration_s": None},
                        {"duration_s": float("nan")}, {"interval_s": -1}, {"processing_delay_s": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await run_live(self.engine, **changes)
        self.assertEqual(self.engine.checkpoint, -1)


class MicrobatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = StreamEngine(Path(self.tmp.name) / "microbatch.sqlite")
        self.records = [Delivery(i, order(f"batch-order-{i}", i * 2, 100 + i)) for i in range(12)]

    async def asyncTearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    async def test_size_flush_partial_final_drain_and_recordwise_equivalence(self):
        observed = []
        async def callback(snapshot):
            await asyncio.sleep(0)
            observed.append(snapshot)
        result = await run_microbatch(
            self.engine, self.records, duration_s=None, interval_s=0,
            max_batch_size=5, max_wait_s=10, queue_maxsize=2, on_snapshot=callback,
        )
        self.assertEqual(result["batch_sizes"], [5, 5, 2])
        self.assertEqual(result["batches"], 3)
        self.assertEqual(result["flush_counts"], {"max_size": 2, "max_wait": 0, "final_drain": 1})
        self.assertEqual(result["final_partial_batch_size"], 2)
        self.assertEqual((result["produced"], result["consumed"]), (12, 12))
        self.assertEqual(result["stop_reason"], "end_of_input")
        self.assertEqual([s["checkpoint"] for s in observed], [4, 9, 11])
        self.assertEqual([s["batch"]["size"] for s in observed], [5, 5, 2])
        self.assertEqual(observed[-1]["queue"]["depth"], 0)
        self.assertNotIn("batch", result["snapshot"])
        self.assertNotIn("queue", self.engine.snapshot())
        self.assertFalse(result["snapshot"]["finalized"])
        with StreamEngine(Path(self.tmp.name) / "individual.sqlite") as individual:
            consume(individual, self.records)
            self.assertEqual(canonical_results(result["snapshot"]), canonical_results(individual.snapshot()))
        self.assertEqual(result["snapshot"]["totals"]["revenue_cents"], 1266)

    async def test_collection_timer_flushes_while_producer_is_still_waiting(self):
        observed = []
        result = await run_microbatch(
            self.engine, self.records[:3], duration_s=None, interval_s=0.04,
            max_batch_size=5, max_wait_s=0.01,
            on_snapshot=lambda snapshot: observed.append(snapshot),
        )
        self.assertEqual(result["batch_sizes"], [1, 1, 1])
        self.assertEqual(result["flush_counts"], {"max_size": 0, "max_wait": 3, "final_drain": 0})
        self.assertTrue(all(wait >= 0.009 for wait in result["batch_wait_s"]))
        self.assertEqual(observed[0]["queue"]["produced"], 1)
        self.assertEqual(observed[0]["queue"]["consumed"], 1)
        self.assertEqual(observed[0]["batch"]["flush_reason"], "max_wait")
        self.assertFalse(result["snapshot"]["finalized"])

    async def test_slow_processing_backpressures_producer_with_bounded_buffers(self):
        result = await run_microbatch(
            self.engine, duration_s=0.05, interval_s=0,
            max_batch_size=4, max_wait_s=0.01, processing_delay_s=0.004, queue_maxsize=2,
        )
        self.assertEqual(result["produced"], result["consumed"])
        self.assertGreater(result["consumed"], 0)
        self.assertGreater(result["backpressure_waits"], 0)
        self.assertLessEqual(result["max_queue_depth"], 2)
        self.assertLessEqual(result["max_buffered_records"], 2 + 4)
        self.assertTrue(all(size <= 4 for size in result["batch_sizes"]))
        self.assertEqual(sum(result["batch_sizes"]), result["consumed"])
        self.assertEqual(result["stop_reason"], "duration")
        self.assertFalse(result["snapshot"]["finalized"])

    async def test_default_source_produces_orders_and_resumes_source_offsets(self):
        first = await run_microbatch(self.engine, duration_s=0.025, interval_s=0.005,
                                     max_batch_size=3, max_wait_s=0.02)
        second = await run_microbatch(self.engine, duration_s=0.025, interval_s=0.005,
                                      max_batch_size=3, max_wait_s=0.02)
        received = first["consumed"] + second["consumed"]
        self.assertGreater(received, 0)
        self.assertEqual(second["snapshot"]["checkpoint"], received - 1)
        self.assertEqual(second["snapshot"]["totals"]["orders"], received)
        self.assertEqual(second["snapshot"]["totals"]["reviews"], 0)
        self.assertEqual(second["event_time_clock"], "synthetic_2s_per_order")

    async def test_mid_batch_failure_keeps_committed_prefix_and_cleans_up_tasks(self):
        original_ingest = self.engine.ingest
        def fail_third(event, *, offset, **kwargs):
            if offset == 2:
                raise RuntimeError("sink failure in the middle of a batch")
            return original_ingest(event, offset=offset, **kwargs)
        self.engine.ingest = fail_third
        try:
            with self.assertRaises(ExceptionGroup) as caught:
                await run_microbatch(self.engine, self.records, duration_s=None, interval_s=0,
                                     max_batch_size=5, max_wait_s=10, queue_maxsize=2)
            self.assertIn("sink failure", str(caught.exception.exceptions[0]))
        finally:
            self.engine.ingest = original_ingest
        self.assertEqual(self.engine.checkpoint, 1)
        self.assertEqual(self.engine.snapshot()["totals"]["orders"], 2)
        running = {task.get_name() for task in asyncio.all_tasks() if not task.done()}
        self.assertNotIn("streaming-lab-microbatch-producer", running)
        self.assertNotIn("streaming-lab-microbatch-consumer", running)
        # Replay includes the committed prefix; it must not double count it.
        consume(self.engine, self.records)
        self.assertEqual(self.engine.snapshot()["totals"]["orders"], 12)
        self.assertEqual(self.engine.snapshot()["totals"]["revenue_cents"], 1266)

    async def test_cancellation_during_callback_cleans_up_and_preserves_commits(self):
        callback_started = asyncio.Event()
        async def pause(snapshot):
            callback_started.set()
            await asyncio.sleep(10)
        task = asyncio.create_task(run_microbatch(
            self.engine, duration_s=5, interval_s=0, max_batch_size=3,
            max_wait_s=10, queue_maxsize=2, on_snapshot=pause,
        ))
        await asyncio.wait_for(callback_started.wait(), timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.engine.checkpoint, 2)
        running = {other.get_name() for other in asyncio.all_tasks() if not other.done()}
        self.assertNotIn("streaming-lab-microbatch-producer", running)
        self.assertNotIn("streaming-lab-microbatch-consumer", running)

    async def test_empty_input_and_explicit_window_finalization(self):
        result = await run_microbatch(self.engine, [], duration_s=None, interval_s=0)
        self.assertEqual((result["produced"], result["consumed"], result["batches"]), (0, 0, 0))
        self.assertEqual(result["batch_details"], [])
        self.assertFalse(result["snapshot"]["finalized"])
        result = await run_microbatch(self.engine, self.records[:1], duration_s=None,
                                     interval_s=0, max_wait_s=10, finalize=True)
        self.assertTrue(result["snapshot"]["finalized"])
        self.assertEqual(result["snapshot"]["revenue_windows"][0]["closure_reason"], "demo_end_of_input")
        self.assertEqual(result["snapshot"]["watermark_ms"], epoch_ms(T0))

    async def test_invalid_limits_fail_before_admission(self):
        for changes in ({"max_batch_size": 0}, {"max_batch_size": True}, {"queue_maxsize": 0},
                        {"max_wait_s": 0}, {"max_wait_s": float("nan")}, {"duration_s": None},
                        {"interval_s": -1}, {"processing_delay_s": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await run_microbatch(self.engine, **changes)
        self.assertEqual(self.engine.checkpoint, -1)


if __name__ == "__main__":
    unittest.main()
