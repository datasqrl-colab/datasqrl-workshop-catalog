#!/bin/bash

# Generate small test datasets with proper foreign key relationships
# Strategy: Start with 20 sellers and extract the entire subgraph of related data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR/../qa_data_large"
TARGET_DIR="$SCRIPT_DIR/../testdata"

mkdir -p "$TARGET_DIR"

echo "Generating test data..."
echo "Source: $SOURCE_DIR"
echo "Target: $TARGET_DIR"

duckdb -c "
-- Load source data with explicit types
CREATE TABLE orders AS SELECT * FROM read_json_auto('$SOURCE_DIR/ecommerce-orders-qa.jsonl', columns={
    order_id: 'VARCHAR',
    customer_id: 'VARCHAR',
    order_purchase_timestamp: 'TIMESTAMP',
    order_estimated_delivery_date: 'TIMESTAMP',
    items: 'JSON[]'
});

CREATE TABLE payments AS SELECT * FROM read_json_auto('$SOURCE_DIR/ecommerce-payments-qa.jsonl', columns={
    order_id: 'VARCHAR',
    payment_sequential: 'INTEGER',
    payment_type: 'VARCHAR',
    payment_installments: 'INTEGER',
    payment_value: 'DOUBLE',
    transaction_approved_at: 'TIMESTAMP'
});

CREATE TABLE fulfillment AS SELECT * FROM read_json_auto('$SOURCE_DIR/ecommerce-orderfulfillment-qa.jsonl', columns={
    order_id: 'VARCHAR',
    event_type: 'VARCHAR',
    timestamp: 'TIMESTAMP'
});

CREATE TABLE reviews AS SELECT * FROM read_csv_auto('$SOURCE_DIR/ecommerce-reviews-qa.csv', types={
    'review_id': 'VARCHAR',
    'order_id': 'VARCHAR',
    'review_score': 'INTEGER',
    'review_comment_title': 'VARCHAR',
    'review_comment_message': 'VARCHAR',
    'review_creation_date': 'TIMESTAMP',
    'review_answer_timestamp': 'TIMESTAMP'
});

CREATE TABLE customers AS SELECT * FROM read_csv_auto('$SOURCE_DIR/ecommerce-customers-qa.csv', types={
    'customer_id': 'VARCHAR',
    'customer_unique_id': 'VARCHAR',
    'customer_zip_code_prefix': 'VARCHAR',
    'customer_city': 'VARCHAR',
    'customer_state': 'VARCHAR',
    'updateTime': 'TIMESTAMP'
});

CREATE TABLE sellers AS SELECT * FROM read_csv_auto('$SOURCE_DIR/ecommerce-sellers-qa.csv', types={
    'seller_id': 'VARCHAR',
    'seller_zip_code_prefix': 'VARCHAR',
    'seller_city': 'VARCHAR',
    'seller_state': 'VARCHAR',
    'updateTime': 'TIMESTAMP'
});

CREATE TABLE products AS SELECT * FROM read_csv_auto('$SOURCE_DIR/ecommerce-products-qa.csv', types={
    'product_id': 'VARCHAR',
    'product_category_name': 'VARCHAR',
    'product_name_length': 'INTEGER',
    'product_description_length': 'INTEGER',
    'product_photos_qty': 'INTEGER',
    'product_weight_g': 'INTEGER',
    'product_length_cm': 'INTEGER',
    'product_height_cm': 'INTEGER',
    'product_width_cm': 'INTEGER',
    'updateTime': 'TIMESTAMP'
});

CREATE TABLE geolocation AS SELECT * FROM read_csv_auto('$SOURCE_DIR/ecommerce-geolocation-qa.csv',
    quote='\"', escape='\"',
    types={
        'geolocation_zip_code_prefix': 'VARCHAR',
        'geolocation_lat': 'DOUBLE',
        'geolocation_lng': 'DOUBLE',
        'geolocation_city': 'VARCHAR',
        'geolocation_state': 'VARCHAR',
        'lastUpdatedTime': 'TIMESTAMP'
    }
);

-- =============================================================================
-- STEP 1: Start with 20 sellers (earliest by updateTime)
-- =============================================================================
CREATE TABLE selected_sellers AS
SELECT * FROM sellers
ORDER BY updateTime
LIMIT 20;

-- =============================================================================
-- STEP 2: Find all orders that contain items from these sellers
-- =============================================================================
CREATE TABLE selected_order_ids AS
SELECT DISTINCT o.order_id
FROM orders o, unnest(o.items) AS t(item)
WHERE json_extract_string(item, '\$.seller_id') IN (SELECT seller_id FROM selected_sellers);

CREATE TABLE selected_orders AS
SELECT * FROM orders
WHERE order_id IN (SELECT order_id FROM selected_order_ids);

-- =============================================================================
-- STEP 3: Extract all product_ids from selected orders
-- =============================================================================
CREATE TABLE selected_product_ids AS
SELECT DISTINCT json_extract_string(item, '\$.product_id') AS product_id
FROM selected_orders, unnest(items) AS t(item);

CREATE TABLE selected_products AS
SELECT * FROM products
WHERE product_id IN (SELECT product_id FROM selected_product_ids)
ORDER BY updateTime;

-- =============================================================================
-- STEP 4: Get all customers from selected orders
-- =============================================================================
CREATE TABLE selected_customer_ids AS
SELECT DISTINCT customer_id FROM selected_orders;

CREATE TABLE selected_customers AS
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM selected_customer_ids)
ORDER BY updateTime;

-- =============================================================================
-- STEP 5: Get all payments for selected orders
-- =============================================================================
CREATE TABLE selected_payments AS
SELECT * FROM payments
WHERE order_id IN (SELECT order_id FROM selected_order_ids)
ORDER BY transaction_approved_at;

-- =============================================================================
-- STEP 6: Get all fulfillment events for selected orders
-- =============================================================================
CREATE TABLE selected_fulfillment AS
SELECT * FROM fulfillment
WHERE order_id IN (SELECT order_id FROM selected_order_ids)
ORDER BY timestamp;

-- =============================================================================
-- STEP 7: Get all reviews for selected orders
-- =============================================================================
CREATE TABLE selected_reviews AS
SELECT * FROM reviews
WHERE order_id IN (SELECT order_id FROM selected_order_ids)
ORDER BY review_answer_timestamp;

-- =============================================================================
-- STEP 8: Get geolocation for all zip codes (customers + sellers)
-- =============================================================================
CREATE TABLE selected_zipcodes AS
SELECT DISTINCT customer_zip_code_prefix AS zip FROM selected_customers
UNION
SELECT DISTINCT seller_zip_code_prefix AS zip FROM selected_sellers;

-- Get one geolocation entry per zipcode
CREATE TABLE selected_geolocation AS
SELECT * FROM (
    SELECT g.*,
           ROW_NUMBER() OVER (PARTITION BY g.geolocation_zip_code_prefix ORDER BY g.geolocation_lat) as rn
    FROM geolocation g
    WHERE g.geolocation_zip_code_prefix IN (SELECT zip FROM selected_zipcodes)
) sub
WHERE rn = 1
ORDER BY lastUpdatedTime;

-- =============================================================================
-- EXPORT ALL DATASETS
-- =============================================================================

-- Customers (CSV, ordered by updateTime)
COPY (
    SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, updateTime
    FROM selected_customers
    ORDER BY updateTime
) TO '$TARGET_DIR/ecommerce-customers-test.csv' (HEADER, DELIMITER ',');

-- Sellers (CSV, ordered by updateTime)
COPY (
    SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state, updateTime
    FROM selected_sellers
    ORDER BY updateTime
) TO '$TARGET_DIR/ecommerce-sellers-test.csv' (HEADER, DELIMITER ',');

-- Products (CSV, ordered by updateTime)
COPY (
    SELECT product_id, product_category_name, product_name_length, product_description_length,
           product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, updateTime
    FROM selected_products
    ORDER BY updateTime
) TO '$TARGET_DIR/ecommerce-products-test.csv' (HEADER, DELIMITER ',');

-- Geolocation (CSV, ordered by lastUpdatedTime)
COPY (
    SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state, lastUpdatedTime
    FROM selected_geolocation
    ORDER BY lastUpdatedTime
) TO '$TARGET_DIR/ecommerce-geolocation-test.csv' (HEADER, DELIMITER ',');

-- Reviews (CSV, ordered by review_answer_timestamp)
COPY (
    SELECT review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
    FROM selected_reviews
    ORDER BY review_answer_timestamp
) TO '$TARGET_DIR/ecommerce-reviews-test.csv' (HEADER, DELIMITER ',');

-- Orders (JSONL, ordered by order_purchase_timestamp)
COPY (
    SELECT order_id, customer_id, order_purchase_timestamp, order_estimated_delivery_date, items
    FROM selected_orders
    ORDER BY order_purchase_timestamp
) TO '$TARGET_DIR/ecommerce-orders-test.jsonl' (FORMAT JSON);

-- Payments (JSONL, ordered by transaction_approved_at)
COPY (
    SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value, transaction_approved_at
    FROM selected_payments
    ORDER BY transaction_approved_at
) TO '$TARGET_DIR/ecommerce-payments-test.jsonl' (FORMAT JSON);

-- Fulfillment events (JSONL, ordered by timestamp)
COPY (
    SELECT order_id, event_type, timestamp
    FROM selected_fulfillment
    ORDER BY timestamp
) TO '$TARGET_DIR/ecommerce-orderfulfillment-test.jsonl' (FORMAT JSON);
"

echo ""
echo "Test data generated successfully!"
echo "Output files:"
ls -la "$TARGET_DIR"

echo ""
echo "Record counts:"
echo "  Sellers: $(tail -n +2 "$TARGET_DIR/ecommerce-sellers-test.csv" | wc -l | tr -d ' ')"
echo "  Customers: $(tail -n +2 "$TARGET_DIR/ecommerce-customers-test.csv" | wc -l | tr -d ' ')"
echo "  Products: $(tail -n +2 "$TARGET_DIR/ecommerce-products-test.csv" | wc -l | tr -d ' ')"
echo "  Geolocation: $(tail -n +2 "$TARGET_DIR/ecommerce-geolocation-test.csv" | wc -l | tr -d ' ')"
echo "  Reviews: $(tail -n +2 "$TARGET_DIR/ecommerce-reviews-test.csv" | wc -l | tr -d ' ')"
echo "  Orders: $(wc -l < "$TARGET_DIR/ecommerce-orders-test.jsonl" | tr -d ' ')"
echo "  Payments: $(wc -l < "$TARGET_DIR/ecommerce-payments-test.jsonl" | tr -d ' ')"
echo "  Fulfillment events: $(wc -l < "$TARGET_DIR/ecommerce-orderfulfillment-test.jsonl" | tr -d ' ')"
