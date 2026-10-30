MERGE `atlantean-land-415108.crm.customer_orders` AS t
USING (
  SELECT
    * EXCEPT (amount, national_id),
    SAFE_CAST(amount AS NUMERIC) AS amount,
    'gs://atlantean-land-415108-dev-data/demo/customers/{{ ds_nodash }}/' AS _source_uri
  FROM `atlantean-land-415108.raw_crm.customer_orders`
  WHERE _PARTITIONDATE = '{{ ds }}'
    AND order_id IS NOT NULL
    AND SAFE_CAST(amount AS NUMERIC) >= 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY customer_id) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN
  UPDATE SET customer_id = s.customer_id, full_name = s.full_name, email = s.email, phone = s.phone,
    amount = s.amount, _source_uri = s._source_uri, _load_id = '{{ run_id }}', _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, full_name, email, phone, amount, _source_uri, _load_id, _loaded_at)
  VALUES (s.order_id, s.customer_id, s.full_name, s.email, s.phone, s.amount, s._source_uri, '{{ run_id }}', CURRENT_TIMESTAMP())
