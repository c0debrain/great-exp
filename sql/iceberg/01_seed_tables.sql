CREATE NAMESPACE IF NOT EXISTS demo.sales;

DROP TABLE IF EXISTS demo.sales.customers;
CREATE TABLE demo.sales.customers (
  customer_id INT,
  email STRING NOT NULL,
  status STRING NOT NULL
) USING iceberg;

INSERT INTO demo.sales.customers VALUES
  (1, 'alice@example.com', 'active'),
  (2, 'bob@example.com', 'inactive'),
  (3, 'cara@example.com', 'active');

DROP TABLE IF EXISTS demo.sales.products;
CREATE TABLE demo.sales.products (
  product_id INT,
  sku STRING NOT NULL,
  price DECIMAL(10, 2) NOT NULL
) USING iceberg;

INSERT INTO demo.sales.products VALUES
  (101, 'SKU-RED-001', 19.99),
  (102, 'SKU-BLU-002', 45.50),
  (103, 'SKU-GRN-003', 250.00);

DROP TABLE IF EXISTS demo.sales.orders;
CREATE TABLE demo.sales.orders (
  order_id INT,
  customer_id INT,
  total DECIMAL(10, 2) NOT NULL
) USING iceberg;

INSERT INTO demo.sales.orders VALUES
  (1001, 1, 120.50),
  (1002, NULL, 45.00),
  (1003, 3, 1500.00);
