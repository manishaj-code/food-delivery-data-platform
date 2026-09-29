"""Tests for scripts/generate_data.py (FR-001 – FR-007, AC-001 – AC-004)."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from scripts import generate_data as gen
from src.common.constants import (
    CUSTOMERS,
    DATASETS,
    DELIVERY,
    DELIVERY_PARTNERS,
    HISTORICAL_END_DATE,
    ORDERS,
    PAYMENTS,
    RESTAURANTS,
    SOURCE_COLUMNS,
    TIMESTAMP_FORMAT,
)

pytestmark = pytest.mark.unit

SAMPLE = gen.PROFILES["sample"]
SEED = 42
DAY_1 = HISTORICAL_END_DATE + timedelta(days=1)  # previous day = historical window
DAY_2 = HISTORICAL_END_DATE + timedelta(days=2)  # previous day = incremental day


@pytest.fixture(scope="module")
def historical() -> gen.GeneratedData:
    return gen.generate_historical(SAMPLE, SEED)


def _ids(rows: list[dict[str, str]], column: str) -> set[str]:
    return {row[column] for row in rows}


def _parse_ts(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError:
        return None


def _file_hashes(directory: Path) -> dict[str, str]:
    return {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def test_sample_volumes_include_bad_records(historical: gen.GeneratedData) -> None:
    bad = historical.expected_failures
    rows = historical.rows

    customer_bad = sum(v for k, v in bad.items() if k.startswith("DQ-CUS"))
    restaurant_bad = sum(v for k, v in bad.items() if k.startswith("DQ-RES"))
    order_bad = sum(v for k, v in bad.items() if k.startswith("DQ-ORD"))
    assert len(rows[CUSTOMERS]) == SAMPLE.customers + customer_bad
    assert len(rows[RESTAURANTS]) == SAMPLE.restaurants + restaurant_bad
    assert len(rows[DELIVERY_PARTNERS]) == SAMPLE.delivery_partners
    assert len(rows[ORDERS]) == SAMPLE.orders + order_bad
    assert len(rows[PAYMENTS]) == SAMPLE.orders + bad["DQ-PAY-004"] + bad["DQ-PAY-005"]
    assert len(rows[DELIVERY]) == SAMPLE.orders + bad["DQ-DEL-005"]


@pytest.mark.slow
def test_full_profile_meets_required_volumes() -> None:
    data = gen.generate_historical(gen.PROFILES["full"], SEED)

    assert len(data.rows[CUSTOMERS]) >= 10_000
    assert len(data.rows[RESTAURANTS]) >= 500
    assert len(data.rows[DELIVERY_PARTNERS]) >= 1_000
    assert len(data.rows[ORDERS]) >= 100_000
    assert len(data.rows[PAYMENTS]) >= 100_000
    assert len(data.rows[DELIVERY]) >= 100_000
    assert data.expected_failures == gen.HISTORICAL_BAD_RECORDS


def test_generation_is_deterministic(tmp_path: Path) -> None:
    for run in ("a", "b"):
        gen.main(
            ["--mode", "historical", "--profile", "sample", "--output-dir", str(tmp_path / run)]
        )
        gen.main(
            [
                "--mode",
                "incremental",
                "--date",
                str(DAY_1),
                "--profile",
                "sample",
                "--output-dir",
                str(tmp_path / run),
            ]
        )

    hashes_a, hashes_b = _file_hashes(tmp_path / "a"), _file_hashes(tmp_path / "b")
    assert hashes_a == hashes_b
    assert len(hashes_a) == 2 * (len(DATASETS) + 1)  # 6 CSVs + manifest per label


def test_different_seed_changes_output() -> None:
    other = gen.generate_historical(SAMPLE, SEED + 1)

    assert other.rows[ORDERS] != gen.generate_historical(SAMPLE, SEED).rows[ORDERS]


def test_foreign_keys_resolve_except_injected_defects(historical: gen.GeneratedData) -> None:
    rows, bad = historical.rows, historical.expected_failures
    customer_ids = _ids(rows[CUSTOMERS], "customer_id") - {""}
    restaurant_ids = _ids(rows[RESTAURANTS], "restaurant_id")
    partner_ids = _ids(rows[DELIVERY_PARTNERS], "delivery_partner_id")
    order_ids = _ids(rows[ORDERS], "order_id")

    missing_customer = [r for r in rows[ORDERS] if r["customer_id"] not in customer_ids]
    missing_restaurant = [r for r in rows[ORDERS] if r["restaurant_id"] not in restaurant_ids]
    assert len(missing_customer) == bad["DQ-ORD-003"]
    assert all(r["customer_id"] == "" for r in missing_customer)
    assert len(missing_restaurant) == bad["DQ-ORD-005"]
    assert all(r["order_id"] in order_ids for r in rows[PAYMENTS])
    assert all(r["order_id"] in order_ids for r in rows[DELIVERY])
    assert all(r["delivery_partner_id"] in partner_ids for r in rows[DELIVERY])


def test_duplicates_match_manifest(historical: gen.GeneratedData) -> None:
    def extra_copies(dataset: str) -> int:
        key = SOURCE_COLUMNS[dataset][0]
        counts = Counter(r[key] for r in historical.rows[dataset] if r[key])
        return sum(count - 1 for count in counts.values())

    bad = historical.expected_failures
    assert extra_copies(CUSTOMERS) == bad["DQ-CUS-002"]
    assert extra_copies(RESTAURANTS) == bad["DQ-RES-002"]
    assert extra_copies(ORDERS) == bad["DQ-ORD-002"]
    assert extra_copies(PAYMENTS) == 0
    assert extra_copies(DELIVERY) == 0


def test_manifest_lists_every_historical_rule(historical: gen.GeneratedData) -> None:
    assert historical.expected_failures == SAMPLE.bad_records


def test_customers_sign_up_before_their_first_order(historical: gen.GeneratedData) -> None:
    signup = {r["customer_id"]: r["signup_date"] for r in historical.rows[CUSTOMERS]}
    for order in historical.rows[ORDERS]:
        placed = _parse_ts(order["order_date"])
        if placed and order["customer_id"] in signup:
            assert signup[order["customer_id"]] <= placed.date().isoformat()


def test_clean_delivered_orders_have_consistent_payment_and_delivery(
    historical: gen.GeneratedData,
) -> None:
    order_numbers = range(1, SAMPLE.orders + 1)
    orders = {r["order_id"]: r for r in historical.rows[ORDERS]}
    payments = {r["order_id"]: r for r in historical.rows[PAYMENTS] if r["payment_id"][3] == "0"}
    deliveries = {r["order_id"]: r for r in historical.rows[DELIVERY] if r["delivery_id"][3] == "0"}

    for number in order_numbers:
        order_id = f"ORD{number:07d}"
        order, payment, delivery = orders[order_id], payments[order_id], deliveries[order_id]
        assert payment["payment_amount"] == order["order_amount"]
        if order["order_status"] == "DELIVERED":
            assert payment["payment_status"] == "SUCCESS"
            assert delivery["delivery_status"] == "DELIVERED"
            assert delivery["pickup_time"] <= delivery["delivery_time"]
        elif order["order_status"] == "CANCELLED":
            assert payment["payment_status"] in ("REFUNDED", "FAILED")
            assert delivery["delivery_status"] == "FAILED"
            assert delivery["pickup_time"] == delivery["delivery_time"] == ""


def test_orders_peak_at_lunch_and_dinner() -> None:
    data = gen.generate_historical(gen.PROFILES["sample"], SEED)
    hours = Counter(ts.hour for r in data.rows[ORDERS] if (ts := _parse_ts(r["order_date"])))

    top_three = {hour for hour, _ in hours.most_common(3)}
    assert top_three <= {12, 13, 14, 19, 20, 21}


@pytest.mark.parametrize("run_date", [DAY_1, DAY_2])
def test_incremental_contains_only_that_days_orders_and_updates(run_date: date) -> None:
    data = gen.generate_incremental(SAMPLE, SEED, run_date)
    previous_day = run_date - timedelta(days=1)

    new_orders, updates = [], []
    for row in data.rows[ORDERS]:
        placed = _parse_ts(row["order_date"])
        if placed is None:
            continue  # injected invalid date
        assert placed.date() in (run_date, previous_day)
        (new_orders if placed.date() == run_date else updates).append(row)

    assert len(new_orders) >= SAMPLE.daily_orders * 0.9
    assert all(u["order_status"] in ("DELIVERED", "CANCELLED") for u in updates)
    assert all(
        r["signup_date"] == run_date.isoformat()
        for r in data.rows[CUSTOMERS][: SAMPLE.daily_new_customers]
    )


def test_incremental_updates_previous_days_in_flight_orders() -> None:
    day_1 = gen.generate_incremental(SAMPLE, SEED, DAY_1)
    day_2 = gen.generate_incremental(SAMPLE, SEED, DAY_2)

    def is_clean(row: dict[str, str]) -> bool:
        return not row["order_id"].startswith("ORD9")  # ORD9xxxxxx = injected bad rows

    in_flight_day_1 = {
        r["order_id"]
        for r in day_1.rows[ORDERS]
        if is_clean(r) and r["order_status"] in ("PLACED", "PREPARING", "OUT_FOR_DELIVERY")
    }
    updated_on_day_2 = {
        r["order_id"]
        for r in day_2.rows[ORDERS]
        if is_clean(r) and (ts := _parse_ts(r["order_date"])) and ts.date() == DAY_1
    }

    assert in_flight_day_1, "sample seed should produce in-flight orders on day 1"
    assert updated_on_day_2 == in_flight_day_1


def test_incremental_dimension_changes_reference_existing_ids(
    historical: gen.GeneratedData,
) -> None:
    data = gen.generate_incremental(SAMPLE, SEED, DAY_1)
    historical_restaurants = _ids(historical.rows[RESTAURANTS], "restaurant_id")

    assert _ids(data.rows[RESTAURANTS], "restaurant_id") <= historical_restaurants
    assert len(data.rows[CUSTOMERS]) == SAMPLE.daily_new_customers + SAMPLE.daily_customer_updates


def test_incremental_bad_records_keep_quality_above_gate() -> None:
    data = gen.generate_incremental(SAMPLE, SEED, DAY_1)
    for dataset, rule_ids in gen.DAILY_BAD_RULES.items():
        bad = sum(data.expected_failures.get(rule_id, 0) for rule_id in rule_ids)
        assert bad >= 1
        assert (len(data.rows[dataset]) - bad) / len(data.rows[dataset]) >= 0.95


def test_incremental_date_must_follow_historical_window() -> None:
    with pytest.raises(ValueError, match="after"):
        gen.generate_incremental(SAMPLE, SEED, HISTORICAL_END_DATE)


def test_cli_writes_csvs_with_expected_headers_and_manifest(tmp_path: Path) -> None:
    assert (
        gen.main(["--mode", "historical", "--profile", "sample", "--output-dir", str(tmp_path)])
        == 0
    )

    for dataset in DATASETS:
        with (tmp_path / dataset / f"{dataset}_historical.csv").open(encoding="utf-8") as handle:
            assert tuple(next(csv.reader(handle))) == SOURCE_COLUMNS[dataset]
    manifest = json.loads((tmp_path / "_bad_records_manifest_historical.json").read_text())
    assert manifest["expected_failures"] == SAMPLE.bad_records
    assert manifest["row_counts"][ORDERS] == SAMPLE.orders + sum(
        v for k, v in SAMPLE.bad_records.items() if k.startswith("DQ-ORD")
    )


def test_cli_requires_date_for_incremental() -> None:
    with pytest.raises(SystemExit):
        gen.parse_args(["--mode", "incremental"])
