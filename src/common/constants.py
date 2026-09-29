"""Reference values shared by the generator, ingestion, validation, and tests.

Single source of truth for dataset names, source columns, and allowed values
(docs/spec/05-data-model-specification.md).
"""

from __future__ import annotations

from datetime import date

# Datasets in dependency order: parents before children.
CUSTOMERS = "customers"
RESTAURANTS = "restaurants"
DELIVERY_PARTNERS = "delivery_partners"
ORDERS = "orders"
PAYMENTS = "payments"
DELIVERY = "delivery"

DATASETS: tuple[str, ...] = (CUSTOMERS, RESTAURANTS, DELIVERY_PARTNERS, ORDERS, PAYMENTS, DELIVERY)

SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    CUSTOMERS: ("customer_id", "customer_name", "email", "city", "signup_date"),
    RESTAURANTS: ("restaurant_id", "restaurant_name", "city", "cuisine", "rating"),
    DELIVERY_PARTNERS: ("delivery_partner_id", "partner_name", "city", "joining_date"),
    ORDERS: (
        "order_id",
        "customer_id",
        "restaurant_id",
        "order_date",
        "order_amount",
        "order_status",
    ),
    PAYMENTS: (
        "payment_id",
        "order_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "payment_date",
    ),
    DELIVERY: (
        "delivery_id",
        "order_id",
        "delivery_partner_id",
        "pickup_time",
        "delivery_time",
        "delivery_status",
    ),
}

BUSINESS_KEYS: dict[str, str] = {dataset: columns[0] for dataset, columns in SOURCE_COLUMNS.items()}

# Metadata columns added by ingestion (FR-013).
INGESTION_METADATA_COLUMNS: tuple[str, ...] = (
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_file",
    "_source_row_number",
    "_run_id",
)

ORDER_STATUSES: tuple[str, ...] = (
    "PLACED",
    "PREPARING",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "CANCELLED",
)
IN_FLIGHT_ORDER_STATUSES: tuple[str, ...] = ("PLACED", "PREPARING", "OUT_FOR_DELIVERY")
PAYMENT_METHODS: tuple[str, ...] = ("UPI", "CARD", "CASH", "WALLET")
PAYMENT_STATUSES: tuple[str, ...] = ("SUCCESS", "FAILED", "REFUNDED")
DELIVERY_STATUSES: tuple[str, ...] = ("ASSIGNED", "PICKED_UP", "DELIVERED", "FAILED")

CITIES: tuple[str, ...] = (
    "Pune",
    "Mumbai",
    "Bengaluru",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Ahmedabad",
)
CUISINES: tuple[str, ...] = (
    "Indian",
    "South Indian",
    "Chinese",
    "Italian",
    "Mughlai",
    "Fast Food",
    "Continental",
    "Desserts",
)

# Source file formats (spec 05 §1).
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Historical load window (spec 05 §3).
HISTORICAL_START_DATE = date(2026, 1, 1)
HISTORICAL_END_DATE = date(2026, 8, 31)
HISTORICAL_FILE_LABEL = "historical"

# Pipeline load types (spec 07 §5).
LOAD_TYPE_HISTORICAL = "historical"
LOAD_TYPE_INCREMENTAL = "incremental"
LOAD_TYPES: tuple[str, ...] = (LOAD_TYPE_HISTORICAL, LOAD_TYPE_INCREMENTAL)

# Business thresholds (spec 02 §5, spec 06 §5).
LATE_DELIVERY_THRESHOLD_MINUTES = 45
DEFAULT_DQ_MIN_QUALITY_SCORE = 95.0


def source_file_name(dataset: str, label: str) -> str:
    """Source CSV name, e.g. ``orders_historical.csv`` or ``orders_2026-09-29.csv``."""
    return f"{dataset}_{label}.csv"
