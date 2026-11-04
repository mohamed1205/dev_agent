CREATE TABLE IF NOT EXISTS sales.daily_branch_sales (
  sales_date DATE NOT NULL OPTIONS(description = "Sales day in Saudi local time (Asia/Riyadh)"),
  branch_code STRING NOT NULL OPTIONS(description = "Branch: RUH, JED or DMM"),
  revenue_sar NUMERIC NOT NULL OPTIONS(description = "Revenue in SAR: sales minus refunds (refund amounts are negative)"),
  transaction_count INT64 NOT NULL OPTIONS(description = "Distinct transactions, SALE and REFUND together"),
  sale_count INT64 NOT NULL OPTIONS(description = "Distinct SALE transactions"),
  refund_count INT64 NOT NULL OPTIONS(description = "Distinct REFUND transactions"),
  _source_uri STRING OPTIONS(description = "The run's source folder the day's rows were loaded from"),
  _load_id STRING OPTIONS(description = "The Airflow run that built the row"),
  _loaded_at TIMESTAMP OPTIONS(description = "When it was built, UTC")
)
PARTITION BY sales_date
CLUSTER BY branch_code
OPTIONS(description = "Revenue and transaction counts per branch per day, for the Finance dashboard. One row per branch and day. Written by pipeline sales-pos-daily.")
