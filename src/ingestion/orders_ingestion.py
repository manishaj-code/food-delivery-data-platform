"""Ingestion of the `orders` source (FR-010)."""

from src.common.constants import ORDERS, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class OrdersIngestion(BaseIngestion):
    config = IngestionConfig(dataset=ORDERS, expected_columns=SOURCE_COLUMNS[ORDERS])
