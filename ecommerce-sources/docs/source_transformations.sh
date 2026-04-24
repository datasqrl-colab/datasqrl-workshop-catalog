#!/bin/bash

# DuckDB Transformations for E-commerce Data
# This script processes CSV files from original_data_large and writes transformed data to qa_data_large

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR/../original_data_large"
TARGET_DIR="$SCRIPT_DIR/../qa_data_large"

# Create target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

echo "Processing e-commerce data transformations..."
echo "Source: $SOURCE_DIR"
echo "Target: $TARGET_DIR"

# =============================================================================
# 1. CUSTOMERS DATASET
# Add 'updateTime' column starting at 2016-01-01 01:00:00, incrementing by 1 minute per record
# This ensures monotonically increasing timestamps for streaming ingestion
# =============================================================================
echo "Transforming customers dataset..."
duckdb -c "
COPY (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        TIMESTAMP '2016-01-01 01:00:00' + INTERVAL (row_number() OVER () - 1) MINUTE AS updateTime
    FROM read_csv_auto('$SOURCE_DIR/olist_customers_dataset.csv')
) TO '$TARGET_DIR/ecommerce-customers-qa.csv' (HEADER, DELIMITER ',');
"

# =============================================================================
# 2. SELLERS DATASET
# Add 'updateTime' column starting at 2016-01-01 01:00:00, incrementing by 1 minute per record
# =============================================================================
echo "Transforming sellers dataset..."
duckdb -c "
COPY (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        TIMESTAMP '2016-01-01 01:00:00' + INTERVAL (row_number() OVER () - 1) MINUTE AS updateTime
    FROM read_csv_auto('$SOURCE_DIR/olist_sellers_dataset.csv')
) TO '$TARGET_DIR/ecommerce-sellers-qa.csv' (HEADER, DELIMITER ',');
"

# =============================================================================
# 3. PRODUCTS DATASET
# - Join with category name translation to get English category names
# - Add 'updateTime' column starting at 2016-01-01 01:00:00, incrementing by 1 minute per record
# - Result is monotonically increasing on updateTime
# =============================================================================
echo "Transforming products dataset..."
duckdb -c "
COPY (
    SELECT
        p.product_id,
        COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name,
        p.product_name_lenght AS product_name_length,
        p.product_description_lenght AS product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        TIMESTAMP '2016-01-01 01:00:00' + INTERVAL (row_number() OVER () - 1) MINUTE AS updateTime
    FROM read_csv_auto('$SOURCE_DIR/olist_products_dataset.csv') p
    LEFT JOIN read_csv_auto('$SOURCE_DIR/product_category_name_translation.csv') t
        ON p.product_category_name = t.product_category_name
) TO '$TARGET_DIR/ecommerce-products-qa.csv' (HEADER, DELIMITER ',');
"

# =============================================================================
# 4. GEOLOCATION DATASET
# Add 'lastUpdatedTime' column with static value 2016-01-01 00:00:00
# =============================================================================
echo "Transforming geolocation dataset..."
duckdb -c "
COPY (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        TIMESTAMP '2016-01-01 00:00:00' AS lastUpdatedTime
    FROM read_csv_auto('$SOURCE_DIR/olist_geolocation_dataset.csv')
) TO '$TARGET_DIR/ecommerce-geolocation-qa.csv' (HEADER, DELIMITER ',');
"

# =============================================================================
# 5. REVIEWS DATASET
# Sorted by review_answer_timestamp ascending for monotonically increasing ingestion
# =============================================================================
echo "Transforming reviews dataset..."
duckdb -c "
COPY (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM read_csv_auto('$SOURCE_DIR/olist_order_reviews_dataset.csv')
    ORDER BY review_answer_timestamp ASC
) TO '$TARGET_DIR/ecommerce-reviews-qa.csv' (HEADER, DELIMITER ',');
"

# =============================================================================
# 6. ORDERS DATASET WITH NESTED ITEMS
# - Join orders with order_items as nested JSON array
# - Includes estimated delivery timestamp
# - Payments are now in a separate table
# - Actual delivery timestamps are in order_fulfillment_event table
# - Output as JSONL (one JSON object per line)
# =============================================================================
echo "Transforming orders dataset with nested items..."
duckdb -c "
COPY (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_estimated_delivery_date,
        COALESCE(items.items, '[]'::JSON) AS items
    FROM read_csv_auto('$SOURCE_DIR/olist_orders_dataset.csv') o
    LEFT JOIN (
        SELECT
            order_id,
            json_group_array(json_object(
                'order_item_id', order_item_id,
                'product_id', product_id,
                'seller_id', seller_id,
                'shipping_limit_date', shipping_limit_date,
                'price', price,
                'freight_value', freight_value
            )) AS items
        FROM read_csv_auto('$SOURCE_DIR/olist_order_items_dataset.csv')
        GROUP BY order_id
    ) items ON o.order_id = items.order_id
) TO '$TARGET_DIR/ecommerce-orders-qa.jsonl' (FORMAT JSON);
"

# =============================================================================
# 7. PAYMENTS DATASET
# - Join payments with orders to get order_approved_at timestamp
# - Sorted by order_approved_at ascending for monotonically increasing ingestion
# - Output as JSONL (one JSON object per line)
# =============================================================================
echo "Transforming payments dataset..."
duckdb -c "
COPY (
    SELECT
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        o.order_approved_at AS transaction_approved_at
    FROM read_csv_auto('$SOURCE_DIR/olist_order_payments_dataset.csv') p
    LEFT JOIN read_csv_auto('$SOURCE_DIR/olist_orders_dataset.csv') o
        ON p.order_id = o.order_id
    ORDER BY o.order_approved_at ASC
) TO '$TARGET_DIR/ecommerce-payments-qa.jsonl' (FORMAT JSON);
"

# =============================================================================
# 8. ORDER FULFILLMENT EVENTS DATASET
# - Break out delivery timestamps from orders into separate events
# - Each event has: order_id, event_type, timestamp
# - Event types: shipped (carrier delivery), delivered (customer delivery)
# - Sorted by timestamp ascending for monotonically increasing ingestion
# - Output as JSONL (one JSON object per line)
# =============================================================================
echo "Transforming order fulfillment events..."
duckdb -c "
COPY (
    SELECT * FROM (
        SELECT
            order_id,
            'shipped' AS event_type,
            order_delivered_carrier_date AS timestamp
        FROM read_csv_auto('$SOURCE_DIR/olist_orders_dataset.csv')
        WHERE order_delivered_carrier_date IS NOT NULL

        UNION ALL

        SELECT
            order_id,
            'delivered' AS event_type,
            order_delivered_customer_date AS timestamp
        FROM read_csv_auto('$SOURCE_DIR/olist_orders_dataset.csv')
        WHERE order_delivered_customer_date IS NOT NULL
    )
    ORDER BY timestamp ASC
) TO '$TARGET_DIR/ecommerce-orderfulfillment-qa.jsonl' (FORMAT JSON);
"

echo "All transformations completed successfully!"
echo "Output files in: $TARGET_DIR"
ls -la "$TARGET_DIR"
