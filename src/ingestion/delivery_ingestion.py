"""Ingestion of the `delivery` source (FR-010)."""

from src.common.constants import DELIVERY, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class DeliveryIngestion(BaseIngestion):
    config = IngestionConfig(dataset=DELIVERY, expected_columns=SOURCE_COLUMNS[DELIVERY])
