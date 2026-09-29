"""Ingestion of the `restaurants` source (FR-010)."""

from src.common.constants import RESTAURANTS, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class RestaurantsIngestion(BaseIngestion):
    config = IngestionConfig(dataset=RESTAURANTS, expected_columns=SOURCE_COLUMNS[RESTAURANTS])
