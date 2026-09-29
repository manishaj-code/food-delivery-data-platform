"""Raw ingestion: one small module per dataset on top of ``BaseIngestion``."""

from src.common.constants import (
    CUSTOMERS,
    DELIVERY,
    DELIVERY_PARTNERS,
    ORDERS,
    PAYMENTS,
    RESTAURANTS,
)
from src.ingestion.base_ingestion import BaseIngestion, IngestionResult
from src.ingestion.customers_ingestion import CustomersIngestion
from src.ingestion.delivery_ingestion import DeliveryIngestion
from src.ingestion.delivery_partners_ingestion import DeliveryPartnersIngestion
from src.ingestion.orders_ingestion import OrdersIngestion
from src.ingestion.payments_ingestion import PaymentsIngestion
from src.ingestion.restaurants_ingestion import RestaurantsIngestion

INGESTORS: dict[str, type[BaseIngestion]] = {
    CUSTOMERS: CustomersIngestion,
    RESTAURANTS: RestaurantsIngestion,
    DELIVERY_PARTNERS: DeliveryPartnersIngestion,
    ORDERS: OrdersIngestion,
    PAYMENTS: PaymentsIngestion,
    DELIVERY: DeliveryIngestion,
}

__all__ = ["INGESTORS", "BaseIngestion", "IngestionResult"]
