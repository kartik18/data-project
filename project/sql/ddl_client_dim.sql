CREATE TABLE IF NOT EXISTS client_dim (
    client_name TEXT TEXT NOT NULL,
    delivery_address TEXT NOT NULL,
    delivery_city TEXT NOT NULL,
    delivery_postcode TEXT NOT NULL,
    delivery_country TEXT NOT NULL,
    delivery_contact_number TEXT,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    is_current BOOLEAN NOT NULL

)