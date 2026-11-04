-- The run day's rows for Finance HQ, as Parquet, staged in Cloud Storage for the copy to S3.
-- Only these six columns leave the warehouse: no customer data, no provenance columns.
EXPORT DATA
  OPTIONS (
    uri = 'gs://atlantean-land-415108-dev-data/finance/daily_branch_sales/{{ ds }}/daily_branch_sales_*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
SELECT
  sales_date,
  branch_code,
  revenue_sar,
  transaction_count,
  sale_count,
  refund_count
FROM `atlantean-land-415108.sales.daily_branch_sales`
WHERE sales_date = DATE '{{ ds }}'
ORDER BY branch_code
