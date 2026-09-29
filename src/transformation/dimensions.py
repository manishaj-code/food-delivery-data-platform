"""Dimension transforms: customers, restaurants, delivery_partners (FR-031 – FR-033).

Input: the run's ``validated/`` DataFrame (typed source columns + ingestion metadata).
Output: the processed schema of spec 07 §6.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.constants import BUSINESS_KEYS, CUSTOMERS, DELIVERY_PARTNERS, RESTAURANTS
from src.transformation.cleaning import conform, deduplicate, round_decimal, trim_and_nullify
from src.transformation.schemas import PROCESSED_SCHEMAS


def _standardise(df: DataFrame, dataset: str, run_id: str) -> DataFrame:
    """Shared steps: trim/NULL, dedupe by business key, add lineage columns."""
    return (
        deduplicate(trim_and_nullify(df), BUSINESS_KEYS[dataset])
        .withColumn("source_ingestion_date", F.col("_ingestion_date"))
        .withColumn("_run_id", F.lit(run_id))
    )


def transform_customers(validated: DataFrame, run_id: str) -> DataFrame:
    df = _standardise(validated, CUSTOMERS, run_id)
    return conform(df, PROCESSED_SCHEMAS[CUSTOMERS])


def transform_restaurants(validated: DataFrame, run_id: str) -> DataFrame:
    df = _standardise(validated, RESTAURANTS, run_id)
    df = df.withColumn("rating", round_decimal(F.col("rating"), 2, 1))
    return conform(df, PROCESSED_SCHEMAS[RESTAURANTS])


def transform_delivery_partners(validated: DataFrame, run_id: str) -> DataFrame:
    df = _standardise(validated, DELIVERY_PARTNERS, run_id)
    return conform(df, PROCESSED_SCHEMAS[DELIVERY_PARTNERS])
