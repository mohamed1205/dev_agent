-- Fails the run (every column must be true) before the day is exported to AWS.
-- Returns only true/false per rule, never a value.
WITH duplicate_days AS (
  SELECT sales_date, branch_code
  FROM `atlantean-land-415108.sales.daily_branch_sales`
  GROUP BY sales_date, branch_code
  HAVING COUNT(*) > 1
),
day_transactions AS (
  SELECT COALESCE(SUM(amount), 0) AS amount_total
  FROM `atlantean-land-415108.sales.pos_transactions`
  WHERE transaction_date = DATE '{{ ds }}'
)
SELECT
  COUNT(*) = 3 AS three_rows_for_run_date,
  COUNT(DISTINCT d.branch_code) = 3 AS one_row_per_branch,
  COUNTIF(d.branch_code IS NULL OR d.branch_code NOT IN ('RUH', 'JED', 'DMM')) = 0 AS branch_code_known,
  COUNTIF(d.revenue_sar IS NULL) = 0 AS revenue_present,
  COALESCE(SUM(d.revenue_sar), 0) = ANY_VALUE(x.amount_total) AS revenue_matches_sales_and_refunds,
  (SELECT COUNT(*) FROM duplicate_days) = 0 AS one_row_per_branch_and_day
FROM `atlantean-land-415108.sales.daily_branch_sales` AS d
CROSS JOIN day_transactions AS x
WHERE d.sales_date = DATE '{{ ds }}'
