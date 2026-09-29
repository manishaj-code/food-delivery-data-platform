"""Ingestion of the `payments` source (FR-010)."""

from src.common.constants import PAYMENTS, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class PaymentsIngestion(BaseIngestion):
    config = IngestionConfig(dataset=PAYMENTS, expected_columns=SOURCE_COLUMNS[PAYMENTS])
