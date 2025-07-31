CREATE TABLE branches (
    branch_id CHAR(36) PRIMARY KEY,
    branch_name VARCHAR(50) UNIQUE
);

CREATE TABLE transactions (
    transaction_id CHAR(36) PRIMARY KEY,
    branch_id CHAR(36),
    transaction_date TIMESTAMP,
    payment_type VARCHAR(20),
    total_cost DECIMAL(8,2),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
);

CREATE TABLE products (
    product_id CHAR(36) PRIMARY KEY,
    product_name VARCHAR(100),
    product_price DECIMAL(8,2)
);

CREATE TABLE order_items (
    item_id CHAR(36) PRIMARY KEY,
    transaction_id CHAR(36),
    product_id CHAR(36),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
