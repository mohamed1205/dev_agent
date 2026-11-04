-- raw_pos.pos_transactions: new table, from bigquery_tables/raw_pos/pos_transactions.sql
CREATE TABLE IF NOT EXISTS raw_pos.pos_transactions (
  transaction_id STRING OPTIONS(description = "Transaction id from the branch's POS, as sent"),
  branch_code STRING OPTIONS(description = "Branch that made the sale (RUH, JED or DMM), as sent"),
  transaction_date STRING OPTIONS(description = "Transaction date in Saudi local time, as sent: YYYY-MM-DD, or DD/MM/YYYY from JED since 2026-11-02"),
  transaction_time STRING OPTIONS(description = "Transaction time in Saudi local time, HH:MM:SS (24h), as sent"),
  transaction_type STRING OPTIONS(description = "SALE or REFUND, as sent"),
  product_sku STRING OPTIONS(description = "Product SKU, as sent"),
  quantity STRING OPTIONS(description = "Quantity, as sent"),
  unit_price STRING OPTIONS(description = "Unit price in SAR, as sent"),
  amount STRING OPTIONS(description = "Amount in SAR, as sent; negative for refunds"),
  currency STRING OPTIONS(description = "Currency code, as sent; always SAR"),
  payment_method STRING OPTIONS(description = "Payment method, as sent"),
  customer_phone STRING OPTIONS(description = "Customer phone number, as sent. Personal data: never copied to the sales dataset or the AWS export"),
  cashier_id STRING OPTIONS(description = "Cashier id, as sent")
)
PARTITION BY _PARTITIONDATE
OPTIONS(description = "POS files as they arrive, one partition per run date (the pos/<YYYY-MM-DD>/ folder loaded). Written by pipeline sales-pos-daily.");
