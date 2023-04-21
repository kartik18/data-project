INSERT INTO client_dim
SELECT
    client_name,
    delivery_address,
    delivery_city,
    delivery_postcode,
    delivery_country,
    delivery_contact_number,
    start_date,
    end_date,
    is_current
FROM 
    stg_client_dim    