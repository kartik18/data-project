INSERT INTO orders_fact
SELECT
    order_number,
    client_name,
    product_name,
    product_type,
    unit_price,
    product_quantity,
    total_price,
    currency,
    payment_type,
    payment_billing_code,
    payment_date
FROM stg_orders_fact;    