-- Fails the run (every column must be true). Returns only true/false per rule, never a value.
SELECT
  COUNTIF(transaction_date = DATE '{{ ds }}') > 0 AS has_rows_for_run_date,
  COUNT(*) = COUNT(DISTINCT transaction_id) AS transaction_id_unique,
  COUNTIF(amount IS NULL) = 0 AS amount_present,
  COUNTIF(transaction_date IS NULL) = 0 AS transaction_date_present,
  COUNTIF(branch_code IS NULL OR branch_code NOT IN ('RUH', 'JED', 'DMM')) = 0 AS branch_code_known
FROM `atlantean-land-415108.sales.pos_transactions`
