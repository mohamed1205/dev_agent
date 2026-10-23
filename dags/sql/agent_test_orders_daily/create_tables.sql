CREATE TABLE IF NOT EXISTS `atlantean-land-415108.dev_raw.agent_test_orders` (
  order_id STRING OPTIONS(description = "Order id, as sent in the daily order file"),
  amount NUMERIC OPTIONS(description = "Order amount, as sent in the daily order file")
)
PARTITION BY _PARTITIONDATE
OPTIONS(description = "Daily order CSV files from gs://atlantean-land-415108-dev-data/agent-test/orders/YYYYMMDD/, loaded as-is. Each partition is the load date and holds that day's folder.");
