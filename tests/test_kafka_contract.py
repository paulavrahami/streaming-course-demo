"""Broker-independent safety tests; the separate smoke script uses a real broker.

These tests deliberately use the real SQLite engine and a small fake transport
so each failure boundary is observable without requiring Docker in the core CI.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from streaming_lab.engine import StreamEngine
from streaming_lab.kafka_demo import (
    CrashBeforeBrokerCommit, DeliveryTracker, KafkaLabError, _enqueue,
    bind_database, choose_start_offset, decode_payload, encode_event,
    flush_checked, make_event, parser, process_message,
)

T0 = datetime(2026, 11, 1, 10, tzinfo=timezone.utc)
TOPIC = "contract-orders"


@dataclass
class Partition:
    topic: str
    partition: int
    offset: int
    error: object = None


class Message:
    def __init__(self, value, offset=0, *, topic=TOPIC, partition=0, error=None):
        self._value = value
        self._offset = offset
        self._topic = topic
        self._partition = partition
        self._error = error

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def error(self):
        return self._error


def record(number=0, *, kind="order", offset=0, burst=False):
    event = make_event(kind, number, run_id="contract", start_time=T0, burst=burst)
    return Message(encode_event(event)[1], offset=offset)


class KafkaCommitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "state.sqlite"
        self.engine = StreamEngine(self.path)
        self.addCleanup(self.engine.close)
        self.consumer = Mock()
        self.consumer.commit.return_value = []

    def process(self, message, **kwargs):
        return process_message(self.engine, self.consumer, message, topic=TOPIC,
                               topic_partition=Partition, **kwargs)

    def test_commit_is_offset_plus_one_after_durable_sqlite_effect(self):
        observed = []

        def inspect_durable_state(*, offsets, asynchronous):
            # A new connection can already see the transaction when Kafka commits.
            with StreamEngine(self.path) as other:
                observed.append(other.snapshot())
            self.assertEqual(offsets, [Partition(TOPIC, 0, 1)])
            self.assertFalse(asynchronous)
            return offsets

        self.consumer.commit.side_effect = inspect_durable_state
        self.process(record())
        self.assertEqual(observed[0]["checkpoint"], 0)
        self.assertEqual(observed[0]["totals"]["orders"], 1)

    def test_sqlite_failure_does_not_commit_kafka(self):
        with patch.object(self.engine, "ingest", side_effect=RuntimeError("disk failure")):
            with self.assertRaisesRegex(RuntimeError, "disk failure"):
                self.process(record())
        self.consumer.commit.assert_not_called()
        self.assertEqual(self.engine.snapshot()["checkpoint"], -1)

    def test_injected_failure_reopens_and_replays_without_double_counting(self):
        with self.assertRaises(CrashBeforeBrokerCommit):
            self.process(record(), crash_before_broker_commit=True)
        before = self.engine.snapshot()
        self.consumer.commit.assert_not_called()
        self.engine.close()
        self.engine = StreamEngine(self.path)
        self.addCleanup(self.engine.close)
        result = self.process(record())
        after = self.engine.snapshot()
        self.assertEqual(result["status"], "replayed")
        self.assertEqual(after["stats"], before["stats"])
        self.assertEqual(after["totals"], before["totals"])
        self.consumer.commit.assert_called_once_with(
            offsets=[Partition(TOPIC, 0, 1)], asynchronous=False)

    def test_logical_duplicate_at_new_offset_is_acknowledged_once(self):
        self.process(record(offset=0))
        self.process(record(offset=1))
        state = self.engine.snapshot()
        self.assertEqual(state["totals"]["orders"], 1)
        self.assertEqual(state["stats"]["duplicates"], 1)
        self.assertEqual(state["checkpoint"], 1)
        self.assertEqual(self.consumer.commit.call_args.kwargs["offsets"][0].offset, 2)

    def test_commit_exception_keeps_local_effect_and_surfaces_failure(self):
        self.consumer.commit.side_effect = RuntimeError("coordinator unavailable")
        with self.assertRaisesRegex(KafkaLabError, "Kafka commit failed"):
            self.process(record())
        self.assertEqual(self.engine.snapshot()["checkpoint"], 0)
        self.consumer.commit.side_effect = None
        self.assertEqual(self.process(record())["status"], "replayed")
        self.assertEqual(self.engine.snapshot()["totals"]["orders"], 1)

    def test_partition_commit_error_is_not_reported_as_success(self):
        self.consumer.commit.return_value = [Partition(TOPIC, 0, 1, "authorization error")]
        with self.assertRaisesRegex(KafkaLabError, "authorization error"):
            self.process(record())
        self.assertEqual(self.engine.snapshot()["checkpoint"], 0)

    def test_bad_payloads_are_durable_dead_letters_before_acknowledging(self):
        for offset, payload in enumerate((None, b"not-json", b"[]", b"\xff", b"{}")):
            self.process(Message(payload, offset=offset))
        state = self.engine.snapshot()
        self.assertEqual(state["stats"]["invalid"], 5)
        self.assertEqual(len(state["dead_letters"]), 5)
        self.assertEqual(state["checkpoint"], 4)
        self.assertEqual(self.consumer.commit.call_count, 5)

    def test_wrong_source_or_broker_error_has_no_local_effect(self):
        for change in ({"topic": "other"}, {"partition": 1}, {"error": "read failed"}):
            with self.subTest(change=change), self.assertRaises(KafkaLabError):
                self.process(Message(record().value(), **change))
        self.assertEqual(self.engine.snapshot()["checkpoint"], -1)
        self.consumer.commit.assert_not_called()

    def test_gap_in_source_offsets_fails_before_broker_commit(self):
        with self.assertRaises(ValueError):
            self.process(record(offset=2))
        self.consumer.commit.assert_not_called()
        self.assertEqual(self.engine.snapshot()["checkpoint"], -1)


class KafkaResumeTests(unittest.TestCase):
    def test_resume_and_crash_replay(self):
        for checkpoint, committed, low, high, expected in (
            (-1, -1001, 0, 5, 0), (4, 5, 0, 5, 5),
            (4, 3, 0, 9, 3), (4, -1001, 0, 9, 0),
        ):
            with self.subTest(checkpoint=checkpoint, committed=committed):
                self.assertEqual(choose_start_offset(checkpoint=checkpoint,
                    committed=committed, low=low, high=high), expected)

    def test_missing_data_or_mismatched_state_is_never_silently_skipped(self):
        for args in (
            {"checkpoint": -1, "committed": 5, "low": 0, "high": 10},
            {"checkpoint": -1, "committed": -1001, "low": 2, "high": 10},
            {"checkpoint": 5, "committed": 6, "low": 0, "high": 2},
            {"checkpoint": 5, "committed": 1, "low": 3, "high": 10},
        ):
            with self.subTest(args=args), self.assertRaises(KafkaLabError):
                choose_start_offset(**args)

    def test_database_binding_rejects_another_group_topic_or_cluster(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite"
            args = dict(bootstrap="localhost:19092", cluster_id="a", topic=TOPIC,
                        group="first", checkpoint=-1)
            bind_database(path, **args)
            bind_database(path, **args)
            for change in ({"group": "other"}, {"topic": "other"}, {"cluster_id": "b"}):
                with self.subTest(change=change), self.assertRaises(KafkaLabError):
                    bind_database(path, **(args | change))

    def test_nonempty_local_database_cannot_be_bound_to_kafka(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(KafkaLabError, "nonempty DB"):
                bind_database(Path(directory) / "state.sqlite", bootstrap="localhost:19092",
                              cluster_id="a", topic=TOPIC, group="g", checkpoint=0)


class KafkaProducerTests(unittest.TestCase):
    def test_event_id_and_payload_are_stable_for_a_run(self):
        args = dict(run_id="fixed", start_time=T0, interval=0.1, seed=3)
        self.assertEqual(make_event("order", 1, **args), make_event("order", 1, **args))
        self.assertNotEqual(make_event("order", 1, **args).event_id,
                            make_event("order", 2, **args).event_id)

    def test_order_key_is_event_id_and_review_key_is_product(self):
        order = make_event("order", 0, run_id="keys", start_time=T0)
        first = make_event("review", 0, run_id="keys", start_time=T0, burst=True)
        second = make_event("review", 1, run_id="keys", start_time=T0, burst=True)
        self.assertEqual(encode_event(order)[0], order.event_id.encode())
        self.assertEqual(encode_event(first)[0], b"5")
        self.assertEqual(encode_event(first)[0], encode_event(second)[0])
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertEqual(json.loads(encode_event(order)[1])["amount_cents"], order.amount_cents)

    def test_delivery_errors_and_unflushed_records_fail(self):
        tracker = DeliveryTracker()
        producer = Mock()
        producer.flush.return_value = 2
        with self.assertRaisesRegex(KafkaLabError, "unconfirmed"):
            flush_checked(producer, tracker, 1)
        producer.flush.return_value = 0
        tracker.callback("delivery timeout", None)
        with self.assertRaisesRegex(KafkaLabError, "delivery failure"):
            flush_checked(producer, tracker, 1)

    def test_flush_dispatches_and_checks_callbacks(self):
        tracker = DeliveryTracker()
        producer = Mock()

        def flush(_timeout):
            tracker.callback("late delivery error", None)
            return 0

        producer.flush.side_effect = flush
        with self.assertRaisesRegex(KafkaLabError, "late delivery error"):
            flush_checked(producer, tracker, 1)

    def test_error_collection_is_bounded(self):
        tracker = DeliveryTracker()
        for _ in range(100):
            tracker.callback("failure", None)
        self.assertEqual(tracker.failed, 100)
        self.assertEqual(len(tracker.first_errors), 5)

    def test_full_producer_queue_has_a_deadline(self):
        producer = Mock()
        producer.produce.side_effect = BufferError("full")
        with patch("streaming_lab.kafka_demo.time.monotonic", side_effect=[0, 1]):
            with self.assertRaisesRegex(KafkaLabError, "enqueue deadline"):
                _enqueue(producer, DeliveryTracker(), TOPIC, b"k", b"v", 1)

    def test_mode_flags_are_explicit_and_mutually_exclusive(self):
        command = parser().parse_args(["produce", "--count", "5"])
        self.assertEqual(command.count, 5)
        self.assertFalse(command.continuous)
        with patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                parser().parse_args(["produce", "--count", "5", "--continuous"])
            with self.assertRaises(SystemExit):
                parser().parse_args(["produce", "--interval", "nan"])

    def test_transport_error_preview_is_bounded(self):
        payload = decode_payload(b"x" * 100_000)
        self.assertLessEqual(len(payload["raw_utf8_preview"]), 4096)


if __name__ == "__main__":
    unittest.main()
