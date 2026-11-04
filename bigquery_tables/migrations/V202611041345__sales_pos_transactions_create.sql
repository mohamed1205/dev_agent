-- sales.pos_transactions: new table, from bigquery_tables/sales/pos_transactions.sql
CREATE TABLE IF NOT EXISTS sales.pos_transactions (
  transaction_id STRING NOT NULL OPTIONS(description = "Transaction id from the branch's POS; unique in this table"),
  branch_code STRING NOT NULL OPTIONS(description = "Branch that made the sale: RUH, JED or DMM"),
  transaction_date DATE NOT NULL OPTIONS(description = "Transaction date in Saudi local time (Asia/Riyadh), parsed from the branch's format"),
  transaction_time TIME OPTIONS(description = "Transaction time in Saudi local time (Asia/Riyadh)"),
  transaction_type STRING NOT NULL OPTIONS(description = "SALE or REFUND"),
  product_sku STRING OPTIONS(description = "Product SKU"),
  quantity NUMERIC OPTIONS(description = "Quantity"),
  unit_price NUMERIC OPTIONS(description = "Unit price in SAR"),
  amount NUMERIC NOT NULL OPTIONS(description = "Amount in SAR, exact decimal; negative for refunds"),
  currency STRING NOT NULL OPTIONS(description = "Currency code; always SAR"),
  payment_method STRING OPTIONS(description = "Payment method"),
  cashier_id STRING OPTIONS(description = "Cashier id"),
  _source_uri STRING OPTIONS(description = "The run's source folder the row was loaded from"),
  _load_id STRING OPTIONS(description = "The Airflow run that loaded it"),
  _loaded_at TIMESTAMP OPTIONS(description = "When it was loaded, UTC")
)
PARTITION BY transaction_date
CLUSTER BY transaction_id
OPTIONS(description = "One clean row per POS transaction, without customer phone numbers. Written by pipeline sales-pos-daily.");
