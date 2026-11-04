-- Replaces the run day's rows in one transaction, so the dashboard never sees a half-written day.
-- One row per branch, even on a day a branch has no transactions (revenue 0).
BEGIN TRANSACTION;

DELETE FROM `atlantean-land-415108.sales.daily_branch_sales`
WHERE sales_date = DATE '{{ ds }}';

INSERT INTO `atlantean-land-415108.sales.daily_branch_sales` (
  sales_date, branch_code, revenue_sar, transaction_count, sale_count, refund_count,
  _source_uri, _load_id, _loaded_at
)
WITH branches AS (
  SELECT code AS branch_code
  FROM UNNEST(['RUH', 'JED', 'DMM']) AS code
)
SELECT
  DATE '{{ ds }}' AS sales_date,
  b.branch_code,
  -- Refunds are stored with negative amounts, so they reduce revenue.
  COALESCE(SUM(p.amount), 0) AS revenue_sar,
  COUNT(DISTINCT p.transaction_id) AS transaction_count,
  COUNT(DISTINCT IF(p.transaction_type = 'SALE', p.transaction_id, NULL)) AS sale_count,
  COUNT(DISTINCT IF(p.transaction_type = 'REFUND', p.transaction_id, NULL)) AS refund_count,
  'gs://atlantean-land-415108-dev-data/pos/{{ ds }}/' AS _source_uri,
  '{{ run_id }}' AS _load_id,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM branches AS b
LEFT JOIN `atlantean-land-415108.sales.pos_transactions` AS p
  ON p.branch_code = b.branch_code
  AND p.transaction_date = DATE '{{ ds }}'
GROUP BY b.branch_code;

COMMIT TRANSACTION;
