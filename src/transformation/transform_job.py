"""Transformation job for one run date (FR-030 – FR-037, FR-082).

Reads only this run's ``validated/`` partitions; parents that arrived on earlier days
come from ``processed/`` (partitions before the run date). Returns the eight processed
DataFrames, cached and counted; writing and publishing happen in Phase 6.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from src.common.config import Settings
from src.common.constants import (
    BUSINESS_KEYS,
    CUSTOMERS,
    DATASETS,
    DELIVERY,
    DELIVERY_PARTNERS,
    ORDERS,
    PAYMENTS,
    RESTAURANTS,
)
from src.common.exceptions import SourceFileError
from src.common.logging_config import log_context
from src.common.paths import Zone, partition_path, spark_uri
from src.common.storage import Storage
from src.transformation.analytics import build_daily_order_metrics, build_order_analytics
from src.transformation.dimensions import (
    transform_customers,
    transform_delivery_partners,
    transform_restaurants,
)
from src.transformation.facts import transform_delivery, transform_orders, transform_payments
from src.transformation.join_validation import assert_no_orphans
from src.transformation.processed_reader import current_state, read_processed
from src.transformation.schemas import DAILY_ORDER_METRICS, ORDER_ANALYTICS, PROCESSED_DATASETS

logger = logging.getLogger(__name__)


@dataclass
class TransformationResult:
    run_date: date
    run_id: str
    datasets: dict[str, DataFrame]
    counts: dict[str, int] = field(default_factory=dict)

    def release(self) -> None:
        for df in self.datasets.values():
            df.unpersist()


def read_validated(
    spark: SparkSession, storage: Storage, settings: Settings, dataset: str, run_date: date
) -> DataFrame:
    prefix = partition_path(Zone.VALIDATED, dataset, run_date)
    if not any(key.endswith(".parquet") for key in storage.list(prefix)):
        raise SourceFileError(
            "Validated partition not found; run validation first", dataset=dataset, key=prefix
        )
    return spark.read.parquet(spark_uri(prefix, settings))


def build_processed(
    validated: dict[str, DataFrame],
    earlier: dict[str, DataFrame | None],
    run_id: str,
) -> dict[str, DataFrame]:
    """Pure transformation: validated batch (+ earlier processed parents) -> 8 datasets."""
    customers = transform_customers(validated[CUSTOMERS], run_id)
    restaurants = transform_restaurants(validated[RESTAURANTS], run_id)
    partners = transform_delivery_partners(validated[DELIVERY_PARTNERS], run_id)
    orders = transform_orders(validated[ORDERS], run_id)
    payments = transform_payments(validated[PAYMENTS], run_id)

    def state(batch: DataFrame, dataset: str) -> DataFrame:
        return current_state(batch, earlier.get(dataset), BUSINESS_KEYS[dataset])

    current_customers = state(customers, CUSTOMERS)
    current_restaurants = state(restaurants, RESTAURANTS)
    current_partners = state(partners, DELIVERY_PARTNERS)
    current_orders = state(orders, ORDERS)
    delivery = transform_delivery(validated[DELIVERY], current_orders, run_id)

    checks = (
        (orders, current_customers, "customer_id", ORDERS, CUSTOMERS),
        (orders, current_restaurants, "restaurant_id", ORDERS, RESTAURANTS),
        (payments, current_orders, "order_id", PAYMENTS, ORDERS),
        (delivery, current_orders, "order_id", DELIVERY, ORDERS),
        (delivery, current_partners, "delivery_partner_id", DELIVERY, DELIVERY_PARTNERS),
    )
    for child, parent_df, column, dataset, parent in checks:
        assert_no_orphans(child, parent_df, column, BUSINESS_KEYS[parent], dataset, parent)

    order_analytics = build_order_analytics(
        orders, payments, delivery, current_customers, current_restaurants, run_id
    )
    return {
        CUSTOMERS: customers,
        RESTAURANTS: restaurants,
        DELIVERY_PARTNERS: partners,
        ORDERS: orders,
        PAYMENTS: payments,
        DELIVERY: delivery,
        ORDER_ANALYTICS: order_analytics,
        DAILY_ORDER_METRICS: build_daily_order_metrics(order_analytics, run_id),
    }


def run_transformations(
    spark: SparkSession, storage: Storage, settings: Settings, run_date: date, run_id: str
) -> TransformationResult:
    """Transform the run's validated partitions into the processed datasets (not yet written)."""
    with log_context(run_id=run_id, run_date=run_date.isoformat()):
        logger.info("Starting transformation")
        validated = {
            dataset: read_validated(spark, storage, settings, dataset, run_date)
            for dataset in DATASETS
        }
        earlier = {
            dataset: read_processed(spark, storage, settings, dataset, before=run_date)
            for dataset in (CUSTOMERS, RESTAURANTS, DELIVERY_PARTNERS, ORDERS)
        }
        datasets = {
            name: df.persist(StorageLevel.MEMORY_AND_DISK)
            for name, df in build_processed(validated, earlier, run_id).items()
        }
        result = TransformationResult(run_date, run_id, datasets)
        for name in PROCESSED_DATASETS:
            result.counts[name] = datasets[name].count()
            logger.info("Transformed %s: %d records", name, result.counts[name])
        logger.info("Transformation completed")
        return result
