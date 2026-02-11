-- Create database
CREATE DATABASE IF NOT EXISTS cdc_db;
USE cdc_db;

-- Create products table with tracking columns
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_last_updated (last_updated),
    INDEX idx_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert initial sample data
INSERT INTO products (name, description, price, stock) VALUES
('Laptop', 'High-performance laptop with 16GB RAM', 1200.00, 50),
('Mouse', 'Wireless ergonomic mouse', 25.00, 200),
('Keyboard', 'Mechanical gaming keyboard', 75.00, 100);

-- Create audit table for tracking all changes
CREATE TABLE IF NOT EXISTS products_audit (
    audit_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    operation VARCHAR(10) NOT NULL,
    old_data JSON,
    new_data JSON,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_product_id (product_id),
    INDEX idx_changed_at (changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
