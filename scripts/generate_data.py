"""Deterministic synthetic data generator.

Implements FR-001 – FR-007 (docs/spec/05-data-model-specification.md).

Modes
-----
historical   one file set covering HISTORICAL_START_DATE → HISTORICAL_END_DATE
incremental  one day (``--date``) of new orders/customers plus status updates of the
             previous day's in-flight orders and a few dimension changes

Every random stream is seeded from ``(seed, purpose)``, so the same arguments always
produce byte-identical files. Intentionally bad records are appended and listed in a
manifest (``_bad_records_manifest_<label>.json``) by data-quality rule ID.

Usage::

    python -m scripts.generate_data --mode historical
    python -m scripts.generate_data --mode incremental --date 2026-09-01
    python -m scripts.generate_data --mode historical --profile sample --output-dir data/sample
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import logging
import os
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

from src.common.constants import (
    CITIES,
    CUISINES,
    CUSTOMERS,
    DATASETS,
    DATE_FORMAT,
    DELIVERY,
    DELIVERY_PARTNERS,
    HISTORICAL_END_DATE,
    HISTORICAL_FILE_LABEL,
    HISTORICAL_START_DATE,
    IN_FLIGHT_ORDER_STATUSES,
    ORDERS,
    PAYMENT_METHODS,
    PAYMENTS,
    RESTAURANTS,
    SOURCE_COLUMNS,
    TIMESTAMP_FORMAT,
    source_file_name,
)
from src.common.logging_config import configure_logging

logger = logging.getLogger("scripts.generate_data")

Row = dict[str, str]  # all values as strings; "" means NULL

FIRST_NAMES = (
    "Rahul",
    "Priya",
    "Amit",
    "Sneha",
    "Vikram",
    "Anjali",
    "Rohan",
    "Neha",
    "Arjun",
    "Kavya",
    "Karan",
    "Pooja",
    "Siddharth",
    "Aisha",
    "Manish",
    "Divya",
    "Aditya",
    "Meera",
    "Nikhil",
    "Riya",
    "Suresh",
    "Lakshmi",
    "Harsh",
    "Ishita",
    "Varun",
    "Tanvi",
    "Deepak",
    "Shreya",
    "Gaurav",
    "Nisha",
)
LAST_NAMES = (
    "Sharma",
    "Patil",
    "Iyer",
    "Reddy",
    "Gupta",
    "Mehta",
    "Nair",
    "Joshi",
    "Kulkarni",
    "Singh",
    "Das",
    "Shah",
    "Rao",
    "Verma",
    "Pillai",
    "Chopra",
    "Bose",
    "Desai",
    "Menon",
    "Agarwal",
)
RESTAURANT_PREFIXES = (
    "Spice",
    "Royal",
    "Green",
    "Urban",
    "Golden",
    "Tandoor",
    "Coastal",
    "Masala",
    "Little",
    "Blue",
)
RESTAURANT_SUFFIXES = (
    "Kitchen",
    "Bistro",
    "Dhaba",
    "Cafe",
    "House",
    "Grill",
    "Express",
    "Corner",
    "Point",
    "Table",
)
# Relative order volume per hour of day: lunch (12–14h) and dinner (19–22h) peaks (FR-007).
HOUR_WEIGHTS = (
    1.0,
    0.5,
    0.3,
    0.2,
    0.2,
    0.3,
    0.6,
    1.5,
    3.0,
    3.5,
    4.0,
    6.0,
    10.0,
    11.0,
    9.0,
    5.0,
    4.0,
    5.0,
    7.0,
    10.0,
    12.0,
    11.0,
    8.0,
    4.0,
)
PAYMENT_METHOD_WEIGHTS = (45, 25, 15, 15)  # UPI, CARD, CASH, WALLET
DELIVERED_SHARE = 0.93  # remainder CANCELLED (spec 05 §6)
IN_FLIGHT_FROM_HOUR = 22  # orders placed at/after 22:00 are still in flight at day end

# ID ranges (spec 05 §4). Bad records use reserved ranges so they never collide.
FIRST_CUSTOMER_NUMBER = 10001
DAILY_CUSTOMER_BASE = 50001
BAD_CUSTOMER_BASE = 90001
FIRST_RESTAURANT_NUMBER = 1001
BAD_RESTAURANT_BASE = 9001
UNKNOWN_RESTAURANT_ID = "R9999"
FIRST_PARTNER_NUMBER = 1001
DAILY_PARTNER_BASE = 5000
DAILY_ORDER_BASE = 1_000_000
MAX_DAILY_ORDERS = 1000
BAD_HISTORICAL_BASE = 8_000_001
BAD_DAILY_BASE = 9_000_000

SIGNUP_START = date(2025, 1, 1)
SIGNUP_END = date(2026, 6, 30)
PARTNER_JOIN_START = date(2024, 1, 1)
PARTNER_JOIN_END = date(2025, 12, 31)

HISTORICAL_BAD_RECORDS: dict[str, int] = {
    "DQ-CUS-001": 5,  # NULL customer_id
    "DQ-CUS-002": 5,  # duplicate customer_id
    "DQ-CUS-003": 3,  # NULL city
    "DQ-CUS-004": 3,  # invalid signup_date
    "DQ-RES-002": 2,  # duplicate restaurant_id
    "DQ-RES-003": 3,  # rating out of range
    "DQ-ORD-002": 10,  # duplicate order_id
    "DQ-ORD-003": 10,  # NULL customer_id
    "DQ-ORD-005": 10,  # missing restaurant
    "DQ-ORD-006": 10,  # negative order_amount
    "DQ-ORD-007": 5,  # invalid order_status
    "DQ-ORD-008": 10,  # invalid order_date
    "DQ-PAY-004": 10,  # invalid payment_amount
    "DQ-PAY-005": 5,  # invalid payment_status
    "DQ-DEL-005": 10,  # pickup_time after delivery_time
}
DAILY_BAD_RULES: dict[str, tuple[str, ...]] = {
    ORDERS: ("DQ-ORD-006", "DQ-ORD-005", "DQ-ORD-008"),
    PAYMENTS: ("DQ-PAY-004", "DQ-PAY-005"),
    DELIVERY: ("DQ-DEL-005",),
}


@dataclass(frozen=True)
class Profile:
    """Volumes for one generation profile."""

    customers: int
    restaurants: int
    delivery_partners: int
    orders: int
    daily_orders: int
    daily_new_customers: int
    daily_customer_updates: int
    daily_restaurant_updates: int
    daily_bad_rate: float
    bad_records: dict[str, int] = field(default_factory=dict)


PROFILES: dict[str, Profile] = {
    "full": Profile(
        customers=10_000,
        restaurants=500,
        delivery_partners=1_000,
        orders=100_000,
        daily_orders=500,
        daily_new_customers=20,
        daily_customer_updates=3,
        daily_restaurant_updates=2,
        daily_bad_rate=0.005,
        bad_records=dict(HISTORICAL_BAD_RECORDS),
    ),
    # Small but large enough that one bad record per rule keeps every dataset >= 95% quality.
    "sample": Profile(
        customers=200,
        restaurants=50,
        delivery_partners=30,
        orders=400,
        daily_orders=40,
        daily_new_customers=3,
        daily_customer_updates=1,
        daily_restaurant_updates=1,
        daily_bad_rate=0.02,
        bad_records=dict.fromkeys(HISTORICAL_BAD_RECORDS, 1),
    ),
}


@dataclass(frozen=True)
class OrderSeed:
    """All random choices for one order, so it can be re-rendered with a later status."""

    order_number: int
    placed_at: datetime
    customer_id: str
    restaurant_id: str
    amount: float
    payment_method: str
    delivery_partner_id: str
    payment_delay_seconds: int
    pickup_delay_minutes: float
    delivery_minutes: float
    cancelled_payment_status: str
    final_status: str
    initial_status: str


@dataclass
class GeneratedData:
    """Rows per dataset plus the expected data-quality failures by rule ID."""

    label: str
    rows: dict[str, list[Row]]
    expected_failures: dict[str, int]


@dataclass(frozen=True)
class Dimensions:
    customers: list[Row]
    restaurants: list[Row]
    partners: list[Row]


# --------------------------------------------------------------------------- helpers


def _rng(seed: int, purpose: str) -> random.Random:
    return random.Random(f"{seed}:{purpose}")


def _fmt_date(value: date) -> str:
    return value.strftime(DATE_FORMAT)


def _fmt_ts(value: datetime) -> str:
    return value.strftime(TIMESTAMP_FORMAT)


def _money(value: float) -> str:
    return f"{value:.2f}"


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def _random_time(rng: random.Random, hour: int) -> time:
    return time(hour, rng.randint(0, 59), rng.randint(0, 59))


def _person_name(rng: random.Random) -> tuple[str, str]:
    return rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)


def _by_city(rows: Sequence[Row], id_column: str) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(row["city"], []).append(row[id_column])
    return grouped


# --------------------------------------------------------------------------- dimensions


def _make_customer(number: int, first: str, last: str, city: str, signup: date) -> Row:
    return {
        "customer_id": f"C{number:05d}",
        "customer_name": f"{first} {last}",
        "email": f"{first}.{last}{number}@example.com".lower(),
        "city": city,
        "signup_date": _fmt_date(signup),
    }


def generate_customers(rng: random.Random, count: int) -> list[Row]:
    """Customers ordered by signup date, so IDs grow with signup date."""
    signups = sorted(_random_date(rng, SIGNUP_START, SIGNUP_END) for _ in range(count))
    customers = []
    for index, signup in enumerate(signups):
        first, last = _person_name(rng)
        customers.append(
            _make_customer(FIRST_CUSTOMER_NUMBER + index, first, last, rng.choice(CITIES), signup)
        )
    return customers


def _rating(rng: random.Random) -> str:
    return "" if rng.random() < 0.02 else f"{rng.uniform(2.5, 5.0):.1f}"


def generate_restaurants(rng: random.Random, count: int) -> list[Row]:
    return [
        {
            "restaurant_id": f"R{FIRST_RESTAURANT_NUMBER + index:04d}",
            "restaurant_name": " ".join(
                (rng.choice(RESTAURANT_PREFIXES), rng.choice(RESTAURANT_SUFFIXES))
            ),
            "city": rng.choice(CITIES),
            "cuisine": rng.choice(CUISINES),
            "rating": _rating(rng),
        }
        for index in range(count)
    ]


def _make_partner(number: int, rng: random.Random, joined: date) -> Row:
    first, last = _person_name(rng)
    return {
        "delivery_partner_id": f"DP{number:04d}",
        "partner_name": f"{first} {last}",
        "city": rng.choice(CITIES),
        "joining_date": _fmt_date(joined),
    }


def generate_partners(rng: random.Random, count: int) -> list[Row]:
    return [
        _make_partner(
            FIRST_PARTNER_NUMBER + index,
            rng,
            _random_date(rng, PARTNER_JOIN_START, PARTNER_JOIN_END),
        )
        for index in range(count)
    ]


def generate_dimensions(profile: Profile, seed: int) -> Dimensions:
    return Dimensions(
        customers=generate_customers(_rng(seed, CUSTOMERS), profile.customers),
        restaurants=generate_restaurants(_rng(seed, RESTAURANTS), profile.restaurants),
        partners=generate_partners(_rng(seed, DELIVERY_PARTNERS), profile.delivery_partners),
    )


# --------------------------------------------------------------------------- orders


class OrderFactory:
    """Draws order seeds consistent with customers, restaurants, and partners."""

    def __init__(self, dims: Dimensions) -> None:
        self._restaurants = dims.restaurants
        self._restaurant_city = {r["restaurant_id"]: r["city"] for r in dims.restaurants}
        self._restaurants_by_city = _by_city(dims.restaurants, "restaurant_id")
        self._partners_by_city = _by_city(dims.partners, "delivery_partner_id")
        self._all_partner_ids = [p["delivery_partner_id"] for p in dims.partners]

    def draw(
        self,
        rng: random.Random,
        order_number: int,
        placed_at: datetime,
        customer: Row,
        in_flight: bool,
    ) -> OrderSeed:
        local_restaurants = self._restaurants_by_city.get(customer["city"])
        if local_restaurants and rng.random() < 0.95:
            restaurant_id = rng.choice(local_restaurants)
        else:
            restaurant_id = rng.choice(self._restaurants)["restaurant_id"]
        partners = self._partners_by_city.get(self._restaurant_city[restaurant_id])
        partner_id = rng.choice(partners or self._all_partner_ids)

        final_status = "DELIVERED" if rng.random() < DELIVERED_SHARE else "CANCELLED"
        method = rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0]
        late = rng.random() < 0.10
        return OrderSeed(
            order_number=order_number,
            placed_at=placed_at,
            customer_id=customer["customer_id"],
            restaurant_id=restaurant_id,
            amount=round(rng.triangular(99, 1500, 350), 2),
            payment_method=method,
            delivery_partner_id=partner_id,
            payment_delay_seconds=rng.randint(5, 90),
            pickup_delay_minutes=rng.uniform(10, 30),
            delivery_minutes=rng.uniform(45.5, 60) if late else rng.uniform(15, 45),
            cancelled_payment_status=(
                "REFUNDED" if method != "CASH" and rng.random() < 0.6 else "FAILED"
            ),
            final_status=final_status,
            initial_status=rng.choice(IN_FLIGHT_ORDER_STATUSES) if in_flight else final_status,
        )


def render_order(seed: OrderSeed, status: str) -> tuple[Row, Row, Row]:
    """Order, payment, and delivery rows for ``seed`` in the given order status."""
    number = seed.order_number
    order_id = f"ORD{number:07d}"
    order = {
        "order_id": order_id,
        "customer_id": seed.customer_id,
        "restaurant_id": seed.restaurant_id,
        "order_date": _fmt_ts(seed.placed_at),
        "order_amount": _money(seed.amount),
        "order_status": status,
    }
    payment_status = seed.cancelled_payment_status if status == "CANCELLED" else "SUCCESS"
    payment = {
        "payment_id": f"PAY{number:07d}",
        "order_id": order_id,
        "payment_method": seed.payment_method,
        "payment_amount": _money(seed.amount),
        "payment_status": payment_status,
        "payment_date": _fmt_ts(seed.placed_at + timedelta(seconds=seed.payment_delay_seconds)),
    }
    pickup = seed.placed_at + timedelta(minutes=seed.pickup_delay_minutes)
    delivered = pickup + timedelta(minutes=seed.delivery_minutes)
    pickup_time, delivery_time, delivery_status = {
        "DELIVERED": (_fmt_ts(pickup), _fmt_ts(delivered), "DELIVERED"),
        "CANCELLED": ("", "", "FAILED"),
        "OUT_FOR_DELIVERY": (_fmt_ts(pickup), "", "PICKED_UP"),
    }.get(status, ("", "", "ASSIGNED"))
    delivery = {
        "delivery_id": f"DEL{number:07d}",
        "order_id": order_id,
        "delivery_partner_id": seed.delivery_partner_id,
        "pickup_time": pickup_time,
        "delivery_time": delivery_time,
        "delivery_status": delivery_status,
    }
    return order, payment, delivery


def _order_timestamps(rng: random.Random, days: Sequence[date], count: int) -> list[datetime]:
    day_weights = [1.2 if day.weekday() >= 5 else 1.0 for day in days]
    chosen_days = rng.choices(days, weights=day_weights, k=count)
    hours = rng.choices(range(24), weights=HOUR_WEIGHTS, k=count)
    return sorted(
        datetime.combine(day, _random_time(rng, hour))
        for day, hour in zip(chosen_days, hours, strict=True)
    )


def historical_order_seeds(profile: Profile, seed: int, dims: Dimensions) -> list[OrderSeed]:
    rng = _rng(seed, ORDERS)
    factory = OrderFactory(dims)
    days = [
        HISTORICAL_START_DATE + timedelta(days=offset)
        for offset in range((HISTORICAL_END_DATE - HISTORICAL_START_DATE).days + 1)
    ]
    signup_dates = [c["signup_date"] for c in dims.customers]  # sorted ascending
    seeds = []
    for index, placed_at in enumerate(_order_timestamps(rng, days, profile.orders)):
        eligible = bisect.bisect_right(signup_dates, _fmt_date(placed_at.date())) or 1
        customer = dims.customers[rng.randrange(eligible)]
        in_flight = (
            placed_at.date() == HISTORICAL_END_DATE and placed_at.hour >= IN_FLIGHT_FROM_HOUR
        )
        seeds.append(factory.draw(rng, index + 1, placed_at, customer, in_flight))
    return seeds


def _day_index(run_date: date) -> int:
    return (run_date - HISTORICAL_END_DATE).days


def daily_new_customers(profile: Profile, seed: int, run_date: date) -> list[Row]:
    rng = _rng(seed, f"new-customers:{run_date}")
    base = DAILY_CUSTOMER_BASE + (_day_index(run_date) - 1) * profile.daily_new_customers
    rows = []
    for offset in range(profile.daily_new_customers):
        first, last = _person_name(rng)
        rows.append(_make_customer(base + offset, first, last, rng.choice(CITIES), run_date))
    return rows


def daily_order_seeds(
    profile: Profile, seed: int, dims: Dimensions, run_date: date
) -> list[OrderSeed]:
    rng = _rng(seed, f"orders:{run_date}")
    count = round(profile.daily_orders * rng.uniform(0.9, 1.1))
    if count > MAX_DAILY_ORDERS:
        raise ValueError(f"daily order volume {count} exceeds {MAX_DAILY_ORDERS}")
    customers = dims.customers + daily_new_customers(profile, seed, run_date)
    factory = OrderFactory(dims)
    base = DAILY_ORDER_BASE + (_day_index(run_date) - 1) * MAX_DAILY_ORDERS
    seeds = []
    for index, placed_at in enumerate(_order_timestamps(rng, [run_date], count)):
        in_flight = placed_at.hour >= IN_FLIGHT_FROM_HOUR
        seeds.append(
            factory.draw(rng, base + index + 1, placed_at, rng.choice(customers), in_flight)
        )
    return seeds


# --------------------------------------------------------------------------- bad records


class BadRecordInjector:
    """Appends intentionally invalid rows and counts them per data-quality rule."""

    def __init__(self, rng: random.Random, rows: dict[str, list[Row]], id_base: int) -> None:
        self._rng = rng
        self._rows = rows
        self._next_number = id_base
        self._next_customer = BAD_CUSTOMER_BASE
        self._next_restaurant = BAD_RESTAURANT_BASE
        self._clean = {dataset: list(dataset_rows) for dataset, dataset_rows in rows.items()}
        self.expected: dict[str, int] = {}

    def _pick(self, dataset: str) -> Row:
        return dict(self._rng.choice(self._clean[dataset]))

    def _number(self) -> int:
        number = self._next_number
        self._next_number += 1
        return number

    def _add(self, dataset: str, rule_id: str, row: Row) -> None:
        self._rows[dataset].append(row)
        self.expected[rule_id] = self.expected.get(rule_id, 0) + 1

    def _new_customer(self) -> Row:
        row = self._pick(CUSTOMERS)
        row["customer_id"] = f"C{self._next_customer:05d}"
        self._next_customer += 1
        return row

    def _new_order(self) -> Row:
        row = self._pick(ORDERS)
        row["order_id"] = f"ORD{self._number():07d}"
        return row

    def inject(self, rule_id: str) -> None:
        injector = getattr(self, "_" + rule_id.replace("-", "_").lower())
        injector(rule_id)

    def _dq_cus_001(self, rule_id: str) -> None:
        row = self._new_customer()
        row["customer_id"] = ""
        self._add(CUSTOMERS, rule_id, row)

    def _dq_cus_002(self, rule_id: str) -> None:
        self._add(CUSTOMERS, rule_id, self._pick(CUSTOMERS))

    def _dq_cus_003(self, rule_id: str) -> None:
        row = self._new_customer()
        row["city"] = ""
        self._add(CUSTOMERS, rule_id, row)

    def _dq_cus_004(self, rule_id: str) -> None:
        row = self._new_customer()
        row["signup_date"] = self._rng.choice(("2026-02-30", "not-a-date", "31/01/2026"))
        self._add(CUSTOMERS, rule_id, row)

    def _dq_res_002(self, rule_id: str) -> None:
        self._add(RESTAURANTS, rule_id, self._pick(RESTAURANTS))

    def _dq_res_003(self, rule_id: str) -> None:
        row = self._pick(RESTAURANTS)
        row["restaurant_id"] = f"R{self._next_restaurant:04d}"
        self._next_restaurant += 1
        row["rating"] = self._rng.choice(("7.5", "-1.0", "5.1"))
        self._add(RESTAURANTS, rule_id, row)

    def _dq_ord_002(self, rule_id: str) -> None:
        self._add(ORDERS, rule_id, self._pick(ORDERS))

    def _dq_ord_003(self, rule_id: str) -> None:
        row = self._new_order()
        row["customer_id"] = ""
        self._add(ORDERS, rule_id, row)

    def _dq_ord_005(self, rule_id: str) -> None:
        row = self._new_order()
        row["restaurant_id"] = UNKNOWN_RESTAURANT_ID
        self._add(ORDERS, rule_id, row)

    def _dq_ord_006(self, rule_id: str) -> None:
        row = self._new_order()
        row["order_amount"] = "-" + row["order_amount"]
        self._add(ORDERS, rule_id, row)

    def _dq_ord_007(self, rule_id: str) -> None:
        row = self._new_order()
        row["order_status"] = self._rng.choice(("SHIPPED", "UNKNOWN", "RETURNED"))
        self._add(ORDERS, rule_id, row)

    def _dq_ord_008(self, rule_id: str) -> None:
        row = self._new_order()
        row["order_date"] = self._rng.choice(
            ("2026-02-30 12:00:00", "not-a-date", "2026-13-01 10:00:00")
        )
        self._add(ORDERS, rule_id, row)

    def _dq_pay_004(self, rule_id: str) -> None:
        row = self._pick(PAYMENTS)
        row["payment_id"] = f"PAY{self._number():07d}"
        row["payment_amount"] = self._rng.choice(("-" + row["payment_amount"], "abc"))
        self._add(PAYMENTS, rule_id, row)

    def _dq_pay_005(self, rule_id: str) -> None:
        row = self._pick(PAYMENTS)
        row["payment_id"] = f"PAY{self._number():07d}"
        row["payment_status"] = "PENDING"
        self._add(PAYMENTS, rule_id, row)

    def _dq_del_005(self, rule_id: str) -> None:
        row = self._pick(DELIVERY)
        row["delivery_id"] = f"DEL{self._number():07d}"
        pickup = datetime.combine(HISTORICAL_START_DATE, time(12, 30))
        if row["pickup_time"]:
            pickup = datetime.strptime(row["pickup_time"], TIMESTAMP_FORMAT)
        row["pickup_time"] = _fmt_ts(pickup)
        row["delivery_time"] = _fmt_ts(pickup - timedelta(minutes=10))
        row["delivery_status"] = "DELIVERED"
        self._add(DELIVERY, rule_id, row)


# --------------------------------------------------------------------------- generation


def _empty_rows() -> dict[str, list[Row]]:
    return {dataset: [] for dataset in DATASETS}


def _append_rendered(rows: dict[str, list[Row]], seeds: Sequence[OrderSeed], final: bool) -> None:
    for order_seed in seeds:
        status = order_seed.final_status if final else order_seed.initial_status
        order, payment, delivery = render_order(order_seed, status)
        rows[ORDERS].append(order)
        rows[PAYMENTS].append(payment)
        rows[DELIVERY].append(delivery)


def generate_historical(profile: Profile, seed: int) -> GeneratedData:
    """Historical file set (HISTORICAL_START_DATE → HISTORICAL_END_DATE)."""
    dims = generate_dimensions(profile, seed)
    rows = _empty_rows()
    rows[CUSTOMERS] = [dict(r) for r in dims.customers]
    rows[RESTAURANTS] = [dict(r) for r in dims.restaurants]
    rows[DELIVERY_PARTNERS] = [dict(r) for r in dims.partners]
    _append_rendered(rows, historical_order_seeds(profile, seed, dims), final=False)

    injector = BadRecordInjector(_rng(seed, "bad-records:historical"), rows, BAD_HISTORICAL_BASE)
    for rule_id, count in profile.bad_records.items():
        for _ in range(count):
            injector.inject(rule_id)
    return GeneratedData(HISTORICAL_FILE_LABEL, rows, injector.expected)


def _previous_in_flight(
    profile: Profile, seed: int, dims: Dimensions, run_date: date
) -> list[OrderSeed]:
    previous_day = run_date - timedelta(days=1)
    if previous_day == HISTORICAL_END_DATE:
        seeds = historical_order_seeds(profile, seed, dims)
    else:
        seeds = daily_order_seeds(profile, seed, dims, previous_day)
    return [s for s in seeds if s.initial_status != s.final_status]


def generate_incremental(profile: Profile, seed: int, run_date: date) -> GeneratedData:
    """One day of new and changed records for ``run_date`` (after the historical window)."""
    if run_date <= HISTORICAL_END_DATE:
        raise ValueError(f"incremental date must be after {HISTORICAL_END_DATE}")
    day_index = _day_index(run_date)
    dims = generate_dimensions(profile, seed)
    rng = _rng(seed, f"changes:{run_date}")
    rows = _empty_rows()

    rows[CUSTOMERS].extend(daily_new_customers(profile, seed, run_date))
    for customer in rng.sample(dims.customers, profile.daily_customer_updates):
        changed = dict(customer)
        changed["email"] = f"updated{day_index}.{customer['email']}"
        rows[CUSTOMERS].append(changed)

    for restaurant in rng.sample(dims.restaurants, profile.daily_restaurant_updates):
        changed = dict(restaurant)
        changed["rating"] = f"{rng.uniform(2.5, 5.0):.1f}"
        rows[RESTAURANTS].append(changed)

    if day_index % 7 == 0:
        rows[DELIVERY_PARTNERS].append(_make_partner(DAILY_PARTNER_BASE + day_index, rng, run_date))

    _append_rendered(rows, _previous_in_flight(profile, seed, dims, run_date), final=True)
    _append_rendered(rows, daily_order_seeds(profile, seed, dims, run_date), final=False)

    injector = BadRecordInjector(
        _rng(seed, f"bad-records:{run_date}"), rows, BAD_DAILY_BASE + day_index * 100
    )
    for dataset, rule_ids in DAILY_BAD_RULES.items():
        count = max(1, round(len(rows[dataset]) * profile.daily_bad_rate))
        for position in range(count):
            injector.inject(rule_ids[position % len(rule_ids)])
        logger.debug("Injected %d bad %s records", count, dataset)
    return GeneratedData(_fmt_date(run_date), rows, injector.expected)


# --------------------------------------------------------------------------- output


def write_generated(data: GeneratedData, output_dir: Path, profile_name: str, seed: int) -> None:
    """Write one CSV per dataset plus the bad-records manifest."""
    for dataset in DATASETS:
        path = output_dir / dataset / source_file_name(dataset, data.label)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=SOURCE_COLUMNS[dataset], lineterminator="\n")
            writer.writeheader()
            writer.writerows(data.rows[dataset])
        logger.info("Wrote %d %s records to %s", len(data.rows[dataset]), dataset, path)

    manifest = {
        "label": data.label,
        "profile": profile_name,
        "seed": seed,
        "row_counts": {dataset: len(data.rows[dataset]) for dataset in DATASETS},
        "expected_failures": dict(sorted(data.expected_failures.items())),
        "note": "expected_failures are minimums; cascading referential failures may add more",
    }
    manifest_path = output_dir / f"_bad_records_manifest_{data.label}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote bad-records manifest to %s", manifest_path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic food delivery source data.")
    parser.add_argument("--mode", choices=("historical", "incremental"), required=True)
    parser.add_argument("--date", type=date.fromisoformat, help="YYYY-MM-DD (incremental mode)")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="full")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.environ.get("SOURCE_DATA_PATH", "data/generated")),
    )
    args = parser.parse_args(argv)
    if args.mode == "incremental" and args.date is None:
        parser.error("--date is required for incremental mode")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args(argv)
    profile = PROFILES[args.profile]
    if args.mode == "historical":
        data = generate_historical(profile, args.seed)
    else:
        data = generate_incremental(profile, args.seed, args.date)
    write_generated(data, args.output_dir, args.profile, args.seed)
    logger.info("Generated %s data set (profile=%s, seed=%d)", data.label, args.profile, args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
