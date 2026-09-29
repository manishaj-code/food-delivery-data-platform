"""Ingestion of the `delivery_partners` source (FR-010)."""

from src.common.constants import DELIVERY_PARTNERS, SOURCE_COLUMNS
from src.ingestion.base_ingestion import BaseIngestion, IngestionConfig


class DeliveryPartnersIngestion(BaseIngestion):
    config = IngestionConfig(
        dataset=DELIVERY_PARTNERS, expected_columns=SOURCE_COLUMNS[DELIVERY_PARTNERS]
    )
