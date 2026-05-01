# E-Commerce Data Products

This catalog contains the data source and sink definitions for the e-commerce data products.
Data products are *silver* layer datasets in the Medallion architecture and provide cleaned, normalized, enriched, or otherwise refined datasets for downstream consumers.

**Enriched Orders**: (defined in `enriched_order_item-*.sqrl` files)
* EnrichedOrderItems: Flattened orders into individual order items enriched with customer and product information. In addition, the dataset is cleaned to remove invalid or partially missing records.

Each filename ends with the version. Versions are for specific environments like `test` (for local development and testing against static files) or `prod` for (pre-)production environments using Kafka for data input and output.

## Changelog & History

### Initial Dataset Creation (2026-05-01)

Used the `data-enrichment/order-items-enrichment.sqrl` script to generate the test dataset from the raw source test data.