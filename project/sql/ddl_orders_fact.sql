CREATE TABLE IF NOT EXISTS orders_fact (
    order_number TEXT PRIMARY KEY,
    client_name TEXT NOT NULL,
    product_name TEXT NOT NULL,
    product_type TEXT NOT NULL,
    unit_price REAL NOT NULL,
    product_quantity INTEGER NOT NULL,
    total_price REAL NOT NULL,
    currency TEXT NOT NULL,
    payment_type TEXT NOT NULL,
    payment_billing_code TEXT NOT NULL,
    payment_date TEXT NOT NULL, 
    FOREIGN KEY(client_name) REFERENCES CLIENT_DIM(client_name)   
);

