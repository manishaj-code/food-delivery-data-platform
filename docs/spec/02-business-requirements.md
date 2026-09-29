# 02 — Business Requirements

Related: `01-project-overview.md`, `08-data-warehouse-specification.md` (tables used below), `03-functional-requirements.md` (FR-060–FR-063).

---

## 1. Business Problem

The company cannot see its daily performance. Order, payment, and delivery data is spread across separate exports, contains errors (duplicates, missing references, negative amounts, invalid dates), and has no single trusted definition for metrics such as "revenue" or "cancellation rate". Decisions about restaurants, delivery partners, and cities are made without data.

## 2. Business Objectives

| ID | Objective |
|---|---|
| BO-1 | Give management a daily view of orders, revenue, and average order value. |
| BO-2 | Track operational problems: cancellations, failed payments, late deliveries. |
| BO-3 | Compare restaurant, cuisine, and city performance. |
| BO-4 | Understand customer activity and repeat behaviour. |
| BO-5 | Identify peak ordering periods to plan delivery-partner capacity. |
| BO-6 | Ensure every reported number is based on validated data with a known quality score. |

## 3. Business Questions

| ID | Question | Metric(s) |
|---|---|---|
| BQ-01 | How many orders do we receive per day / month? | M-01 |
| BQ-02 | How much revenue do we make per day / month? | M-02 |
| BQ-03 | What is the average order value? | M-03 |
| BQ-04 | What share of orders is cancelled? | M-04 |
| BQ-05 | What share of payment attempts succeed? Which methods fail most? | M-05 |
| BQ-06 | How long do deliveries take? How many are late? | M-06, M-07 |
| BQ-07 | Which restaurants perform best / worst? | M-08 |
| BQ-08 | How active are customers? How many are repeat customers? | M-09 |
| BQ-09 | Which cities generate the most revenue? | M-10 |
| BQ-10 | Which cuisines are ordered most? | M-11 |
| BQ-11 | When do customers order (hour of day, day of week)? | M-12 |

## 4. Required Analytics

The 15 analytical queries required by `project_details.md` §16 (implemented in `sql/analytics/`, Phase 8):

| # | File | Answers | Metric |
|---|---|---|---|
| 1 | `01_daily_orders.sql` | Orders per day | M-01 |
| 2 | `02_daily_revenue.sql` | Revenue per day | M-02 |
| 3 | `03_average_order_value.sql` | AOV overall and per day | M-03 |
| 4 | `04_cancellation_rate.sql` | Cancellation rate per day and overall | M-04 |
| 5 | `05_payment_success_rate.sql` | Payment success rate per method | M-05 |
| 6 | `06_revenue_by_city.sql` | Revenue per restaurant city | M-10 |
| 7 | `07_orders_by_restaurant.sql` | Orders per restaurant | M-08 |
| 8 | `08_top_restaurants.sql` | Top 10 restaurants by revenue | M-08 |
| 9 | `09_orders_by_cuisine.sql` | Orders and revenue per cuisine | M-11 |
| 10 | `10_average_delivery_time.sql` | Avg delivery minutes overall / per city | M-06 |
| 11 | `11_late_deliveries.sql` | Late deliveries count and rate | M-07 |
| 12 | `12_customer_order_frequency.sql` | Distribution of orders per customer | M-09 |
| 13 | `13_repeat_customers.sql` | Repeat-customer count and rate | M-09 |
| 14 | `14_monthly_revenue.sql` | Revenue per month | M-02 |
| 15 | `15_peak_ordering_hours.sql` | Orders by hour of day and day of week | M-12 |

Power BI views (Phase 8): `vw_daily_orders`, `vw_daily_revenue`, `vw_restaurant_performance`, `vw_delivery_performance`, `vw_customer_summary`, `vw_payment_summary`.

## 5. Global Metric Conventions

These conventions apply to every metric and are the single source of truth for SQL and PySpark calculations.

| Convention | Decision |
|---|---|
| Currency | INR, `DECIMAL(10,2)`. |
| Timezone | All timestamps are UTC. Dates are derived from UTC timestamps. |
| Reporting date for orders | `order_date` (date part of order timestamp). |
| Reporting date for revenue | The **order date** of the order the payment belongs to (keeps orders and revenue on the same calendar). |
| Revenue | Sum of `payment_amount` for payments with `payment_status = 'SUCCESS'`. `FAILED` and `REFUNDED` payments are excluded. |
| Paid order | An order with at least one `SUCCESS` payment. |
| Completed delivery | `delivery_status = 'DELIVERED'` with non-null `pickup_time` and `delivery_time`. |
| Late delivery | Completed delivery with `delivery_duration_minutes > 45` (constant `LATE_DELIVERY_THRESHOLD_MINUTES = 45`). |
| City for revenue/orders | The **restaurant's** city (where the order is fulfilled). |
| Division by zero | Rates return `NULL` when the denominator is 0 (`NULLIF`). |
| Data included | Only records that passed validation and were loaded into the warehouse. |

## 6. Key Business Metrics

### M-01 Total Orders

| Aspect | Specification |
|---|---|
| Definition | Number of distinct orders placed in the period, regardless of status. |
| Calculation | `COUNT(DISTINCT order_id)` from `fact_order`, grouped by `dim_date.full_date` via `order_date_key`. |
| Source data | `fact_order`, `dim_date` |
| Expected output | `order_date, total_orders` — e.g. `2026-09-28, 512`. Exposed in `vw_daily_orders`. |

### M-02 Total Revenue

| Aspect | Specification |
|---|---|
| Definition | Money successfully collected for orders in the period. |
| Calculation | `SUM(fp.payment_amount)` where `fp.payment_status = 'SUCCESS'`, joined `fact_payment fp → fact_order fo` on `order_id`, grouped by `fo.order_date_key` (daily) or year-month (monthly). |
| Source data | `fact_payment`, `fact_order`, `dim_date` |
| Expected output | `order_date, total_revenue` — e.g. `2026-09-28, 214530.75`. Exposed in `vw_daily_revenue`. |

### M-03 Average Order Value (AOV)

| Aspect | Specification |
|---|---|
| Definition | Average revenue per paid order. |
| Calculation | `total_revenue / NULLIF(COUNT(DISTINCT paid order_id), 0)`, rounded to 2 decimals. |
| Source data | `fact_payment`, `fact_order` |
| Expected output | `order_date, paid_orders, total_revenue, average_order_value` — e.g. `…, 470, 214530.75, 456.45`. Also pre-computed in PySpark (`daily_order_metrics`) and reconciled against SQL (FR-026). |

### M-04 Cancellation Rate

| Aspect | Specification |
|---|---|
| Definition | Share of orders whose current status is `CANCELLED`. |
| Calculation | `COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)` over `fact_order`. |
| Source data | `fact_order` |
| Expected output | `order_date, total_orders, cancelled_orders, cancellation_rate_pct` — e.g. `…, 512, 36, 7.03`. |

### M-05 Payment Success Rate

| Aspect | Specification |
|---|---|
| Definition | Share of payment attempts that succeeded. |
| Calculation | `COUNT(CASE WHEN payment_status = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)` over `fact_payment`, optionally grouped by `payment_method`. |
| Source data | `fact_payment` |
| Expected output | `payment_method, total_payments, successful_payments, success_rate_pct` — e.g. `UPI, 41022, 39380, 96.00`. Exposed in `vw_payment_summary`. |

### M-06 Average Delivery Time

| Aspect | Specification |
|---|---|
| Definition | Average minutes between pickup and delivery for completed deliveries. |
| Calculation | `AVG(delivery_duration_minutes)` where `delivery_status = 'DELIVERED'`. `delivery_duration_minutes = (delivery_time − pickup_time)` in minutes, computed in PySpark. |
| Source data | `fact_delivery` (+ `dim_restaurant` via `fact_order` for city) |
| Expected output | `order_date, city, completed_deliveries, avg_delivery_minutes` — e.g. `…, Pune, 88, 31.4`. Exposed in `vw_delivery_performance`. |

### M-07 Late Deliveries

| Aspect | Specification |
|---|---|
| Definition | Completed deliveries that took more than 45 minutes. |
| Calculation | `COUNT(CASE WHEN delivery_duration_minutes > 45 THEN 1 END)` and rate over completed deliveries. |
| Source data | `fact_delivery` |
| Expected output | `order_date, city, late_deliveries, late_delivery_rate_pct`. |

### M-08 Restaurant Performance

| Aspect | Specification |
|---|---|
| Definition | Per-restaurant volume, revenue, cancellations, and delivery speed. |
| Calculation | Group by `restaurant_key`: `COUNT(orders)`, `SUM(successful payment_amount)`, AOV, cancellation rate, `AVG(delivery_duration_minutes)`; plus `rating` from `dim_restaurant`. Top restaurants = `ORDER BY total_revenue DESC LIMIT 10`. |
| Source data | `fact_order`, `fact_payment`, `fact_delivery`, `dim_restaurant` |
| Expected output | `restaurant_id, restaurant_name, city, cuisine, rating, total_orders, total_revenue, average_order_value, cancellation_rate_pct, avg_delivery_minutes`. Exposed in `vw_restaurant_performance` (supports "Rating vs Orders"). |

### M-09 Customer Activity

| Aspect | Specification |
|---|---|
| Definition | Order frequency and spend per customer; repeat customer = customer with ≥ 2 orders. |
| Calculation | Group by `customer_key`: `COUNT(orders)`, `SUM(successful payments)`, `MIN/MAX(order_date)`, `is_repeat_customer = total_orders >= 2`. Repeat rate = repeat customers / customers with ≥ 1 order. |
| Source data | `fact_order`, `fact_payment`, `dim_customer` |
| Expected output | `customer_id, customer_name, city, signup_date, total_orders, total_spend, first_order_date, last_order_date, is_repeat_customer`. Exposed in `vw_customer_summary`. |

### M-10 Revenue by City

| Aspect | Specification |
|---|---|
| Definition | Revenue attributed to the restaurant's city. |
| Calculation | M-02 grouped by `dim_restaurant.city`. |
| Source data | `fact_payment`, `fact_order`, `dim_restaurant` |
| Expected output | `city, total_orders, total_revenue` — e.g. `Bengaluru, 14210, 6480033.10`. |

### M-11 Orders by Cuisine

| Aspect | Specification |
|---|---|
| Definition | Order count and revenue per restaurant cuisine. |
| Calculation | `COUNT(orders)` and M-02 grouped by `dim_restaurant.cuisine`. |
| Source data | `fact_order`, `fact_payment`, `dim_restaurant` |
| Expected output | `cuisine, total_orders, total_revenue, share_of_orders_pct`. |

### M-12 Peak Ordering Periods

| Aspect | Specification |
|---|---|
| Definition | Distribution of orders by hour of day (0–23) and day of week. |
| Calculation | `EXTRACT(HOUR FROM order_timestamp)` and `dim_date.day_name`, `COUNT(*)`, ranked descending. |
| Source data | `fact_order` (`order_timestamp`), `dim_date` |
| Expected output | `order_hour, day_name, total_orders` — synthetic data is generated with lunch (12–14h) and dinner (19–22h) peaks, so these hours should rank highest. |

## 7. Power BI Dashboard Requirements (consumer view)

| Page | Visuals | Source view |
|---|---|---|
| Overview | Total Orders, Total Revenue, AOV, Cancellation Rate, Payment Success Rate, Avg Delivery Time (cards + daily trend) | `vw_daily_orders`, `vw_daily_revenue`, `vw_payment_summary`, `vw_delivery_performance` |
| Restaurant | Top Restaurants, Revenue by Restaurant, Orders by Cuisine, Rating vs Orders | `vw_restaurant_performance` |
| Delivery | Avg Delivery Time, Delivery Time by City, Late Deliveries, Delivery Status | `vw_delivery_performance` |

Power BI files are **not** stored in the repository; only the views and the connection guide (Phase 15) are delivered.
