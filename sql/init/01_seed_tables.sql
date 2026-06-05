CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  status TEXT NOT NULL
);

INSERT INTO customers (customer_id, email, status) VALUES
  (1, 'alice@example.com', 'active'),
  (2, 'bob@example.com', 'inactive'),
  (3, 'cara@example.com', 'active');

CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  sku TEXT NOT NULL,
  price NUMERIC(10, 2) NOT NULL
);

INSERT INTO products (product_id, sku, price) VALUES
  (101, 'SKU-RED-001', 19.99),
  (102, 'SKU-BLU-002', 45.50),
  (103, 'SKU-GRN-003', 250.00);

CREATE TABLE orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER,
  total NUMERIC(10, 2) NOT NULL
);

INSERT INTO orders (order_id, customer_id, total) VALUES
  (1001, 1, 120.50),
  (1002, NULL, 45.00),
  (1003, 3, 1500.00);
