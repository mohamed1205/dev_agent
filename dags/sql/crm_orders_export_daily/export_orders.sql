-- Only non-sensitive columns leave BigQuery: no full_name, email, phone or national_id.
EXPORT DATA OPTIONS (
  uri = 'gs://atlantean-land-415108-dev-data/exports/customer_orders/{{ ds_nodash }}/part-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
SELECT
  order_id,
  customer_id,
  amount,
  _load_id,
  _loaded_at
FROM `atlantean-land-415108.crm.customer_orders`
WHERE _load_id = '{{ run_id }}'
