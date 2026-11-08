"""Synthetic, deterministic input records. Money is always integer cents.

The input envelope's offset belongs to ONE ordered source partition. It is not
part of an event's identity: retries may reuse event_id at a new offset.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Any, Iterable, Mapping

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
T0 = datetime(2026, 11, 1, 10, 0, 0, tzinfo=UTC)
PRICE_CENTS = (4990, 8990, 11900, 22900, 34900)
PRODUCT_BY_PRICE = {4990: 6, 8990: 3, 11900: 5, 22900: 4, 34900: 1}


class InvalidEvent(ValueError):
    """The payload does not satisfy the small teaching schema."""


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise InvalidEvent(f"{name} must be an integer >= {minimum}")
    return value


def parse_time(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise InvalidEvent("event_time must be ISO-8601") from exc
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise InvalidEvent("event_time must include a timezone")
    return value.astimezone(UTC)


def epoch_ms(value: datetime | str) -> int:
    """Convert without floating-point rounding at window boundaries."""
    delta = parse_time(value) - EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def iso_time(value_ms: int | None) -> str | None:
    if value_ms is None:
        return None
    return (EPOCH + timedelta(milliseconds=value_ms)).isoformat(timespec="milliseconds")


def window_start_ms(value: datetime | str | int, window_seconds: int = 60) -> int:
    if isinstance(window_seconds, bool) or not isinstance(window_seconds, int) or window_seconds <= 0:
        raise ValueError("window_seconds must be a positive integer")
    value_ms = value if isinstance(value, int) and not isinstance(value, bool) else epoch_ms(value)
    width = window_seconds * 1000
    return (value_ms // width) * width


@dataclass(frozen=True)
class Event:
    event_id: str
    kind: str
    event_time: datetime
    product_id: int
    amount_cents: int | None = None
    rating: int | None = None
    text: str = ""
    order_id: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise InvalidEvent("event_id must be a nonempty string")
        if self.kind not in ("order", "review"):
            raise InvalidEvent("kind must be 'order' or 'review'")
        object.__setattr__(self, "event_time", parse_time(self.event_time))
        _integer(self.product_id, "product_id", 1)
        if self.kind == "order":
            _integer(self.amount_cents, "amount_cents")
            if self.rating is not None:
                raise InvalidEvent("an order must not contain a rating")
        else:
            _integer(self.rating, "rating", 1)
            if self.rating > 5:
                raise InvalidEvent("rating must be between 1 and 5")
            if self.amount_cents is not None:
                raise InvalidEvent("a review must not contain amount_cents")
        if not isinstance(self.text, str):
            raise InvalidEvent("text must be a string")
        if self.order_id is not None:
            _integer(self.order_id, "order_id", 1)

    def to_dict(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "event_id": self.event_id,
            "kind": self.kind,
            "event_time": self.event_time.isoformat(),
            "product_id": self.product_id,
        }
        if self.kind == "order":
            record["amount_cents"] = self.amount_cents
            if self.order_id is not None:
                record["order_id"] = self.order_id
        else:
            record.update(rating=self.rating, text=self.text)
        return record

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Event:
        if not isinstance(value, Mapping):
            raise InvalidEvent("event must be an object")
        try:
            return cls(
                event_id=value["event_id"], kind=value["kind"],
                event_time=value["event_time"], product_id=value["product_id"],
                amount_cents=value.get("amount_cents"), rating=value.get("rating"),
                text=value.get("text", ""), order_id=value.get("order_id"),
            )
        except KeyError as exc:
            raise InvalidEvent(f"missing required field: {exc.args[0]}") from exc


@dataclass(frozen=True)
class Delivery:
    offset: int
    event: Event | dict[str, Any]
    arrival_delay_s: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {"offset": self.offset, "event": self.event.to_dict() if isinstance(self.event, Event) else self.event,
                "arrival_delay_s": self.arrival_delay_s}


def generate_order_fixture(n: int = 300, seed: int = 7) -> list[Delivery]:
    """Reproduce the old 300-order generator's random draw/arrival pattern.

    ~10% of originals are delayed 20..90 simulated seconds; ~5% are retried.
    The only monetary representation is cents. No network, raw files, or API key.
    arrival_delay_s describes the simulated source timeline, not a sleep request.
    """
    _integer(n, "n")
    rnd = random.Random(seed)
    arrivals: list[tuple[datetime, Event]] = []
    for i in range(n):
        event_time = T0 + timedelta(seconds=i * 2 + rnd.random())
        chance = rnd.random()
        delay = rnd.uniform(20, 90) if chance < 0.10 else rnd.uniform(0, 5)
        price = rnd.choice(PRICE_CENTS)
        quantity = rnd.choice((1, 1, 1, 2))
        event = Event(f"ev-{i:04d}", "order", event_time, PRODUCT_BY_PRICE[price],
                      amount_cents=price * quantity, order_id=5000 + i)
        arrival = event_time + timedelta(seconds=delay)
        arrivals.append((arrival, event))
        if rnd.random() < 0.05:
            arrivals.append((arrival + timedelta(seconds=rnd.uniform(1, 10)), event))
    arrivals.sort(key=lambda pair: pair[0])
    previous = arrivals[0][0] if arrivals else T0
    deliveries = []
    for offset, (arrival, event) in enumerate(arrivals):
        deliveries.append(Delivery(offset, event, (arrival - previous).total_seconds()))
        previous = arrival
    return deliveries


def generate_review_fixture() -> list[Delivery]:
    """Five unique reviews, one producer retry, and one alert threshold crossing."""
    reviews = [
        Event("review-1", "review", T0 + timedelta(seconds=5), 5, rating=1, text="הגיע שבור"),
        Event("review-2", "review", T0 + timedelta(seconds=12), 5, rating=2, text="איכות ירודה"),
        Event("review-3", "review", T0 + timedelta(seconds=15), 5, rating=1, text="לא עובד בכלל"),
        Event("review-4", "review", T0 + timedelta(seconds=18), 5, rating=1, text="הסוללה נגמרת מהר"),
        Event("review-5", "review", T0 + timedelta(seconds=65), 3, rating=5, text="מוצר מעולה"),
    ]
    ordered = reviews[:2] + [reviews[0]] + reviews[2:]
    return [Delivery(i, event) for i, event in enumerate(ordered)]


def teaching_fixture() -> list[Delivery]:
    """17 arrivals with easy-to-audit arithmetic, disorder, retries and bad input.

    With 60-second windows, grace=10, out_of_order=0:
    accepted 7 orders / 12_200 cents, 5 reviews, 2 duplicate IDs, 2 late orders
    (1_300 cents), 1 invalid payload, and 1 review alert. Batch order truth is
    9 orders / 13_500 cents. Finite completion emits four revenue windows.
    """
    def order(key: str, second: int, cents: int) -> Event:
        return Event(key, "order", T0 + timedelta(seconds=second), 5, amount_cents=cents)
    def review(key: str, second: int, rating: int, text: str) -> Event:
        return Event(key, "review", T0 + timedelta(seconds=second), 5, rating=rating, text=text)
    a = order("order-a", 5, 1000)
    late = order("order-late", 25, 500)
    raw: list[Event | dict[str, Any]] = [
        a,
        review("review-1", 10, 1, "הגיע שבור"),
        order("order-b", 65, 2000),
        order("order-c", 20, 1500),
        review("review-2", 12, 2, "איכות ירודה"),
        a,
        review("review-3", 15, 1, "לא עובד בכלל"),
        review("review-4", 18, 1, "הסוללה נגמרת מהר"),
        order("order-d", 75, 3000),
        late,
        order("order-e", 125, 2500),
        {"event_id": "bad-money", "kind": "order", "event_time": (T0 + timedelta(seconds=130)).isoformat(),
         "product_id": 5, "amount_cents": 19.9},
        order("order-f", 135, 1000),
        review("review-5", 125, 5, "מוצר מעולה"),
        late,
        order("order-boundary", 60, 800),
        order("order-h", 195, 1200),
    ]
    return [Delivery(i, event) for i, event in enumerate(raw)]


def batch_truth(deliveries: Iterable[Delivery]) -> dict[str, int]:
    """Ignore arrival order, deduplicate valid IDs, and account for invalid input."""
    unique: dict[str, Event] = {}
    invalid = 0
    for item in deliveries:
        try:
            event = item.event if isinstance(item.event, Event) else Event.from_dict(item.event)
        except (InvalidEvent, TypeError, OverflowError):
            invalid += 1
            continue
        unique.setdefault(event.event_id, event)
    orders = [event for event in unique.values() if event.kind == "order"]
    reviews = [event for event in unique.values() if event.kind == "review"]
    return {"unique_events": len(unique), "orders": len(orders),
            "revenue_cents": sum(event.amount_cents for event in orders),
            "reviews": len(reviews), "negative_reviews": sum(event.rating <= 2 for event in reviews),
            "invalid": invalid}
