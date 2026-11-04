-- Fails the run (every column must be true) before anything reaches the sales dataset.
-- Returns only true/false per rule, never a value from the files.
WITH run_rows AS (
  SELECT
    transaction_id,
    branch_code,
    transaction_type,
    currency,
    amount,
    quantity,
    unit_price,
    transaction_time,
    -- Only the formats each branch's POS sends; anything else counts as unparsed.
    CASE
      WHEN branch_code IN ('RUH', 'DMM') OR (branch_code = 'JED' AND DATE '{{ ds }}' < DATE '2026-11-02')
        THEN IF(REGEXP_CONTAINS(transaction_date, r'^\d{4}-\d{2}-\d{2}$'), SAFE.PARSE_DATE('%Y-%m-%d', transaction_date), NULL)
      WHEN branch_code = 'JED'
        THEN IF(REGEXP_CONTAINS(transaction_date, r'^\d{2}/\d{2}/\d{4}$'), SAFE.PARSE_DATE('%d/%m/%Y', transaction_date), NULL)
    END AS parsed_date
  FROM `atlantean-land-415108.raw_pos.pos_transactions`
  WHERE _PARTITIONDATE = '{{ ds }}'
),
conflicting_ids AS (
  -- A transaction_id seen more than once must be an exact copy (a re-sent file), never different rows.
  SELECT transaction_id
  FROM `atlantean-land-415108.raw_pos.pos_transactions`
  WHERE _PARTITIONDATE = '{{ ds }}'
  GROUP BY transaction_id
  HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(
    branch_code, transaction_date, transaction_time, transaction_type, product_sku,
    quantity, unit_price, amount, currency, payment_method, cashier_id
  ))) > 1
)
SELECT
  COUNT(*) > 0 AS has_rows,
  COUNTIF(transaction_id IS NULL OR TRIM(transaction_id) = '') = 0 AS transaction_id_present,
  COUNTIF(branch_code IS NULL OR branch_code NOT IN ('RUH', 'JED', 'DMM')) = 0 AS branch_code_known,
  COUNTIF(parsed_date IS NULL) = 0 AS transaction_date_parsed,
  COUNTIF(parsed_date != DATE '{{ ds }}') = 0 AS transaction_date_is_run_date,
  COUNTIF(transaction_time IS NOT NULL AND (NOT REGEXP_CONTAINS(transaction_time, r'^\d{2}:\d{2}:\d{2}$') OR SAFE.PARSE_TIME('%H:%M:%S', transaction_time) IS NULL)) = 0 AS transaction_time_parsed,
  COUNTIF(transaction_type IS NULL OR transaction_type NOT IN ('SALE', 'REFUND')) = 0 AS transaction_type_known,
  COUNTIF(currency IS NULL OR currency != 'SAR') = 0 AS currency_is_sar,
  COUNTIF(SAFE_CAST(amount AS NUMERIC) IS NULL) = 0 AS amount_present_and_numeric,
  COUNTIF(quantity IS NOT NULL AND SAFE_CAST(quantity AS NUMERIC) IS NULL) = 0 AS quantity_numeric,
  COUNTIF(unit_price IS NOT NULL AND SAFE_CAST(unit_price AS NUMERIC) IS NULL) = 0 AS unit_price_numeric,
  COUNTIF(transaction_type = 'REFUND' AND SAFE_CAST(amount AS NUMERIC) > 0) = 0 AS refund_amount_not_positive,
  (SELECT COUNT(*) FROM conflicting_ids) = 0 AS duplicate_rows_identical
FROM run_rows
