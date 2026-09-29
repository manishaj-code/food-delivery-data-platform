"""The data-quality rule catalogue: exactly the 34 rules of spec 06 §2 (FR-020)."""

from __future__ import annotations

from src.common.constants import (
    CUSTOMERS,
    DATASETS,
    DELIVERY,
    DELIVERY_PARTNERS,
    DELIVERY_STATUSES,
    ORDER_STATUSES,
    ORDERS,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    PAYMENTS,
    RESTAURANTS,
)
from src.validation.rules import (
    COLUMN_LTE,
    EXISTS_IN,
    IN_SET,
    NOT_NULL,
    NUMERIC_RANGE,
    REQUIRED_WHEN,
    UNIQUE,
    VALID_DATE,
    VALID_TIMESTAMP,
    WARN,
    Rule,
)

RULES: tuple[Rule, ...] = (
    # customers
    Rule("DQ-CUS-001", CUSTOMERS, ("customer_id",), NOT_NULL),
    Rule("DQ-CUS-002", CUSTOMERS, ("customer_id",), UNIQUE),
    Rule("DQ-CUS-003", CUSTOMERS, ("city",), NOT_NULL),
    Rule(
        "DQ-CUS-004", CUSTOMERS, ("signup_date",), VALID_DATE, params={"not_after_run_date": True}
    ),
    Rule("DQ-CUS-005", CUSTOMERS, ("email",), NOT_NULL, severity=WARN),
    # restaurants
    Rule("DQ-RES-001", RESTAURANTS, ("restaurant_id",), NOT_NULL),
    Rule("DQ-RES-002", RESTAURANTS, ("restaurant_id",), UNIQUE),
    Rule(
        "DQ-RES-003",
        RESTAURANTS,
        ("rating",),
        NUMERIC_RANGE,
        params={"min": 0, "max": 5, "allow_null": True},
    ),
    # delivery_partners
    Rule("DQ-DPT-001", DELIVERY_PARTNERS, ("delivery_partner_id",), NOT_NULL),
    Rule("DQ-DPT-002", DELIVERY_PARTNERS, ("delivery_partner_id",), UNIQUE),
    Rule("DQ-DPT-003", DELIVERY_PARTNERS, ("joining_date",), VALID_DATE),
    # orders
    Rule("DQ-ORD-001", ORDERS, ("order_id",), NOT_NULL),
    Rule("DQ-ORD-002", ORDERS, ("order_id",), UNIQUE),
    Rule("DQ-ORD-003", ORDERS, ("customer_id",), NOT_NULL),
    Rule("DQ-ORD-004", ORDERS, ("customer_id",), EXISTS_IN, params={"parent": CUSTOMERS}),
    Rule("DQ-ORD-005", ORDERS, ("restaurant_id",), EXISTS_IN, params={"parent": RESTAURANTS}),
    Rule("DQ-ORD-006", ORDERS, ("order_amount",), NUMERIC_RANGE, params={"min": 0}),
    Rule("DQ-ORD-007", ORDERS, ("order_status",), IN_SET, params={"values": ORDER_STATUSES}),
    Rule(
        "DQ-ORD-008", ORDERS, ("order_date",), VALID_TIMESTAMP, params={"not_after_run_date": True}
    ),
    # payments
    Rule("DQ-PAY-001", PAYMENTS, ("payment_id",), NOT_NULL),
    Rule("DQ-PAY-002", PAYMENTS, ("payment_id",), UNIQUE),
    Rule("DQ-PAY-003", PAYMENTS, ("order_id",), EXISTS_IN, params={"parent": ORDERS}),
    Rule("DQ-PAY-004", PAYMENTS, ("payment_amount",), NUMERIC_RANGE, params={"min": 0}),
    Rule("DQ-PAY-005", PAYMENTS, ("payment_status",), IN_SET, params={"values": PAYMENT_STATUSES}),
    Rule("DQ-PAY-006", PAYMENTS, ("payment_method",), IN_SET, params={"values": PAYMENT_METHODS}),
    Rule("DQ-PAY-007", PAYMENTS, ("payment_date",), VALID_TIMESTAMP),
    # delivery
    Rule("DQ-DEL-001", DELIVERY, ("delivery_id",), NOT_NULL),
    Rule("DQ-DEL-002", DELIVERY, ("delivery_id",), UNIQUE),
    Rule("DQ-DEL-003", DELIVERY, ("order_id",), EXISTS_IN, params={"parent": ORDERS}),
    Rule(
        "DQ-DEL-004",
        DELIVERY,
        ("delivery_partner_id",),
        EXISTS_IN,
        params={"parent": DELIVERY_PARTNERS},
    ),
    Rule("DQ-DEL-005", DELIVERY, ("pickup_time", "delivery_time"), COLUMN_LTE),
    Rule(
        "DQ-DEL-006", DELIVERY, ("delivery_status",), IN_SET, params={"values": DELIVERY_STATUSES}
    ),
    Rule(
        "DQ-DEL-007",
        DELIVERY,
        ("pickup_time", "delivery_time"),
        VALID_TIMESTAMP,
        params={"allow_null": True},
    ),
    Rule(
        "DQ-DEL-008",
        DELIVERY,
        ("delivery_time",),
        REQUIRED_WHEN,
        severity=WARN,
        params={"when_column": "delivery_status", "when_value": "DELIVERED"},
    ),
)


def rules_for(dataset: str) -> tuple[Rule, ...]:
    return tuple(rule for rule in RULES if rule.dataset == dataset)


def parent_datasets(dataset: str) -> tuple[str, ...]:
    """Parents referenced by the dataset's referential rules, in dependency order."""
    parents = {rule.params["parent"] for rule in rules_for(dataset) if rule.check == EXISTS_IN}
    return tuple(name for name in DATASETS if name in parents)
