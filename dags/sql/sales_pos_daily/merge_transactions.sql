-- One row per transaction_id: a file the POS vendor re-sends under a new name is counted once.
-- customer_phone is never selected, so it can't reach the sales dataset.
MERGE `atlantean-land-415108.sales.pos_transactions` AS t
USING (
  SELECT
    transaction_id,
    branch_code,
    -- Strict per-branch parsing: a value in any other format fails the job, never guessed.
    CASE
      WHEN branch_code IN ('RUH', 'DMM') OR (branch_code = 'JED' AND DATE '{{ ds }}' < DATE '2026-11-02')
        THEN PARSE_DATE('%Y-%m-%d', transaction_date)
      WHEN branch_code = 'JED'
        THEN PARSE_DATE('%d/%m/%Y', transaction_date)
      ELSE ERROR('raw_pos.pos_transactions has a branch_code outside RUH, JED, DMM')
    END AS transaction_date,
    PARSE_TIME('%H:%M:%S', transaction_time) AS transaction_time,
    transaction_type,
    product_sku,
    CAST(quantity AS NUMERIC) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(amount AS NUMERIC) AS amount,
    currency,
    payment_method,
    cashier_id,
    'gs://atlantean-land-415108-dev-data/pos/{{ ds }}/' AS _source_uri
  FROM (
    SELECT
      transaction_id, branch_code, transaction_date, transaction_time, transaction_type, product_sku,
      quantity, unit_price, amount, currency, payment_method, cashier_id
    FROM `atlantean-land-415108.raw_pos.pos_transactions`
    WHERE _PARTITIONDATE = '{{ ds }}'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY TO_JSON_STRING(STRUCT(
        branch_code, transaction_date, transaction_time, transaction_type, product_sku,
        quantity, unit_price, amount, currency, payment_method, cashier_id
      ))
    ) = 1
  )
) AS s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN
  UPDATE SET
    branch_code = s.branch_code,
    transaction_date = s.transaction_date,
    transaction_time = s.transaction_time,
    transaction_type = s.transaction_type,
    product_sku = s.product_sku,
    quantity = s.quantity,
    unit_price = s.unit_price,
    amount = s.amount,
    currency = s.currency,
    payment_method = s.payment_method,
    cashier_id = s.cashier_id,
    _source_uri = s._source_uri,
    _load_id = '{{ run_id }}',
    _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id, branch_code, transaction_date, transaction_time, transaction_type, product_sku,
    quantity, unit_price, amount, currency, payment_method, cashier_id,
    _source_uri, _load_id, _loaded_at
  )
  VALUES (
    s.transaction_id, s.branch_code, s.transaction_date, s.transaction_time, s.transaction_type, s.product_sku,
    s.quantity, s.unit_price, s.amount, s.currency, s.payment_method, s.cashier_id,
    s._source_uri, '{{ run_id }}', CURRENT_TIMESTAMP()
  )
