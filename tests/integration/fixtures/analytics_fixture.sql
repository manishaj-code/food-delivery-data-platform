-- Hand-built warehouse fixture for the metric tests (FR-062, AC-082).
-- Expected results are worked out in tests/integration/test_analytics_sql.py.
--
-- 2026-09-01 is a Tuesday, 2026-09-02 a Wednesday.
--
-- Orders (restaurant, date, UTC hour, status) and their payments / delivery:
--   O1 C1 R1 09-01 12h DELIVERED  P1 UPI 500 SUCCESS              D1 30 min DELIVERED
--   O2 C1 R2 09-01 13h DELIVERED  P2 CARD 300 FAILED, P3 UPI 300 SUCCESS  D2 50 min DELIVERED (late)
--   O3 C2 R1 09-01 20h CANCELLED  P4 CARD 200 REFUNDED           D3 FAILED
--   O4 C2 R2 09-01 20h DELIVERED  P5 WALLET 400 SUCCESS          D4 45 min DELIVERED (not late)
--   O5 C3 R1 09-02 12h DELIVERED  P6 CASH 250 SUCCESS            D5 40 min DELIVERED
--   O6 C1 R3 09-02 19h DELIVERED  P7 CARD 650 SUCCESS            D6 55 min DELIVERED (late)
--   O7 C3 R2 09-02 20h CANCELLED  P8 UPI 150 FAILED              (no delivery)
--   O8 C2 R1 09-02 21h PREPARING  P9 UPI 350 SUCCESS paid 09-03  D8 ASSIGNED
--   O9 C4 R3 09-02 13h DELIVERED  P10 CARD 100 SUCCESS           D9 25 min DELIVERED
-- C5 and R4 have no orders.

INSERT INTO {schema}.dim_customer
    (customer_key, customer_id, customer_name, email, city, signup_date,
     source_ingestion_date, created_at, updated_at)
VALUES
    (1, 'C1', 'Asha Rao',     'c1@example.com', 'Pune',   '2026-01-10', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2, 'C2', 'Vikram Shah',  'c2@example.com', 'Mumbai', '2026-02-11', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (3, 'C3', 'Meera Iyer',   'c3@example.com', 'Pune',   '2026-03-12', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (4, 'C4', 'Rohan Das',    NULL,             'Delhi',  '2026-04-13', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (5, 'C5', 'Kavya Nair',   'c5@example.com', 'Delhi',  '2026-05-14', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');

INSERT INTO {schema}.dim_restaurant
    (restaurant_key, restaurant_id, restaurant_name, city, cuisine, rating,
     source_ingestion_date, created_at, updated_at)
VALUES
    (1, 'R1', 'Spice Route',   'Pune',   'Indian',   4.5, '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2, 'R2', 'Dragon Wok',    'Mumbai', 'Chinese',  4.0, '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (3, 'R3', 'Pasta Corner',  'Pune',   'Italian',  3.5, '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (4, 'R4', 'Sweet Tooth',   'Delhi',  'Desserts', 4.8, '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');

INSERT INTO {schema}.dim_delivery_partner
    (delivery_partner_key, delivery_partner_id, partner_name, city, joining_date,
     source_ingestion_date, created_at, updated_at)
VALUES
    (1, 'DP1', 'Arjun Kumar', 'Pune',   '2025-06-01', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2, 'DP2', 'Sana Khan',   'Mumbai', '2025-07-01', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');

INSERT INTO {schema}.fact_order
    (order_key, order_id, customer_key, restaurant_key, order_date_key, order_timestamp,
     order_amount, order_status, source_ingestion_date, created_at, updated_at)
VALUES
    (1, 'O1', 1, 1, 20260901, '2026-09-01 12:30:00', 500.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2, 'O2', 1, 2, 20260901, '2026-09-01 13:15:00', 300.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (3, 'O3', 2, 1, 20260901, '2026-09-01 20:00:00', 200.00, 'CANCELLED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (4, 'O4', 2, 2, 20260901, '2026-09-01 20:45:00', 400.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (5, 'O5', 3, 1, 20260902, '2026-09-02 12:10:00', 250.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (6, 'O6', 1, 3, 20260902, '2026-09-02 19:30:00', 650.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (7, 'O7', 3, 2, 20260902, '2026-09-02 20:20:00', 150.00, 'CANCELLED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (8, 'O8', 2, 1, 20260902, '2026-09-02 21:00:00', 350.00, 'PREPARING', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (9, 'O9', 4, 3, 20260902, '2026-09-02 13:40:00', 100.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');

INSERT INTO {schema}.fact_payment
    (payment_key, payment_id, order_id, payment_method, payment_amount, payment_status,
     payment_date_key, payment_timestamp, source_ingestion_date, created_at, updated_at)
VALUES
    (1,  'P1',  'O1', 'UPI',    500.00, 'SUCCESS',  20260901, '2026-09-01 12:31:00', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2,  'P2',  'O2', 'CARD',   300.00, 'FAILED',   20260901, '2026-09-01 13:16:00', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (3,  'P3',  'O2', 'UPI',    300.00, 'SUCCESS',  20260901, '2026-09-01 13:18:00', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (4,  'P4',  'O3', 'CARD',   200.00, 'REFUNDED', 20260901, '2026-09-01 20:01:00', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (5,  'P5',  'O4', 'WALLET', 400.00, 'SUCCESS',  20260901, '2026-09-01 20:46:00', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (6,  'P6',  'O5', 'CASH',   250.00, 'SUCCESS',  20260902, '2026-09-02 13:10:00', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (7,  'P7',  'O6', 'CARD',   650.00, 'SUCCESS',  20260902, '2026-09-02 19:31:00', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (8,  'P8',  'O7', 'UPI',    150.00, 'FAILED',   20260902, '2026-09-02 20:21:00', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (9,  'P9',  'O8', 'UPI',    350.00, 'SUCCESS',  20260903, '2026-09-03 00:10:00', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (10, 'P10', 'O9', 'CARD',   100.00, 'SUCCESS',  20260902, '2026-09-02 13:41:00', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');

INSERT INTO {schema}.fact_delivery
    (delivery_key, delivery_id, order_id, delivery_partner_key, order_date_key, pickup_time,
     delivery_time, delivery_duration_minutes, delivery_status, source_ingestion_date,
     created_at, updated_at)
VALUES
    (1, 'D1', 'O1', 1, 20260901, '2026-09-01 12:50:00', '2026-09-01 13:20:00', 30.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (2, 'D2', 'O2', 2, 20260901, '2026-09-01 13:35:00', '2026-09-01 14:25:00', 50.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (3, 'D3', 'O3', 2, 20260901, '2026-09-01 20:15:00', NULL,                  NULL,  'FAILED',    '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (4, 'D4', 'O4', 1, 20260901, '2026-09-01 21:05:00', '2026-09-01 21:50:00', 45.00, 'DELIVERED', '2026-09-01', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (5, 'D5', 'O5', 2, 20260902, '2026-09-02 12:30:00', '2026-09-02 13:10:00', 40.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (6, 'D6', 'O6', 1, 20260902, '2026-09-02 19:50:00', '2026-09-02 20:45:00', 55.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (8, 'D8', 'O8', 2, 20260902, NULL,                  NULL,                  NULL,  'ASSIGNED',  '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00'),
    (9, 'D9', 'O9', 1, 20260902, '2026-09-02 14:00:00', '2026-09-02 14:25:00', 25.00, 'DELIVERED', '2026-09-02', '2026-09-03 00:00:00', '2026-09-03 00:00:00');
