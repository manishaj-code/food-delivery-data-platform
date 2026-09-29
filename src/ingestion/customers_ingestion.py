"""Ingestion of the `customers` source (FR-010)."""

from src.common.constants import CUSTOMERS, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class CustomersIngestion(BaseIngestion):
    config = IngestionConfig(dataset=CUSTOMERS, expected_columns=SOURCE_COLUMNS[CUSTOMERS])
