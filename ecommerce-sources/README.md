# E-Commerce Sources

This catalog contains the data source definitions for the core e-commerce data.

**Master Data**: (defined in `masterdata-*.sqrl` files)
* Customers: Customer data CDC stream from the operational customer database
* Sellers: Seller data CDC stream from operational seller database
* Products: Product data CDC stream from operational seller database
* Geolocation: Resolves zip codes to geo-coordinates, extracted from a 3rd static dataset

**Order Data**: (defined in `orders-*.sqrl` files)
* Orders: The actual order by a customer with nested items referencing products
* Payments: Customer payments for orders (may be split or in installments)
* Order Fulfillment: Order fulfillment events (shipped, delivered)
* Reviews: Customer reviews of orders from feedback surveys

![Data Model](docs/olist_data_model.png)

The `-schema-only.sqrl` files contain the base schema for all the table definitions. All other versions are for specific environments like `test` (for local development and testing) or `qa` (for QA and extended testing on larger datasets).

## Changelog & History

### Initial Dataset Creation (2026-04-07)

This dataset is derived from the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data) on Kaggle. It is a real world dataset provided by the E-Commerce company Olist.

The original data from Kaggle was transformed using [this script](docs/source_transformations.sh) to restore the original event stream of data.

The test data is extracted from the entire dataset using [this script](docs/generate_testdata.sh).