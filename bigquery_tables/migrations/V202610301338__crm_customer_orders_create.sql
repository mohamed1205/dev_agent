-- crm.customer_orders: new table, from bigquery_tables/crm/customer_orders.sql
CREATE TABLE IF NOT EXISTS crm.customer_orders (
  order_id STRING NOT NULL OPTIONS(description = "Order id from the source system"),
  customer_id STRING OPTIONS(description = "Customer id"),
  full_name STRING OPTIONS(description = "Customer's full name"),
  email STRING OPTIONS(description = "Customer's email"),
  phone STRING OPTIONS(description = "Customer's phone number"),
  amount NUMERIC OPTIONS(description = "Order amount"),
  _source_uri STRING OPTIONS(description = "The source folder the row was loaded from"),
  _load_id STRING OPTIONS(description = "The Airflow run that loaded it"),
  _loaded_at TIMESTAMP OPTIONS(description = "When it was loaded, UTC")
)
CLUSTER BY order_id;
