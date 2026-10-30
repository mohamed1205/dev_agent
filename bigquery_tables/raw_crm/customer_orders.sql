CREATE TABLE IF NOT EXISTS raw_crm.customer_orders (
  customer_id STRING OPTIONS(description = "Customer id from the source system"),
  full_name STRING OPTIONS(description = "Customer's full name, as sent"),
  email STRING OPTIONS(description = "Customer's email, as sent"),
  phone STRING OPTIONS(description = "Customer's phone number, as sent"),
  national_id STRING OPTIONS(description = "Customer's national id, as sent"),
  order_id STRING OPTIONS(description = "Order id from the source system"),
  amount STRING OPTIONS(description = "Order amount, as sent")
)
PARTITION BY _PARTITIONDATE;
