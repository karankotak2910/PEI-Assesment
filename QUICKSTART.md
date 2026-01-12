PEI Assessment - 

Extract and Setup

1. Extract source files - Orders, Customers, Products

2. Data is located in mnt/pei/source/
   Customer.xlsx, Orders.json, Products.csv

3. Open in PyCharm
   File -> Open -> Select pei_assessment folder

4. Install dependencies
   pip install -r requirements.txt

Run Pipeline

Execute in order:

python notebooks/01_bronze_ingestion.py
python notebooks/02_silver_enrichment.py
python notebooks/03_gold_aggregation.py
python notebooks/04_sql_analytics.py

Run Tests

pytest tests/ -v
pytest tests/ --cov=src

File Paths

All paths use /mnt/pei base directory:

Source:  /mnt/pei/source/{Customer.xlsx, Orders.json, Products.csv}
Bronze:  /mnt/pei/bronze/{customers, products, orders}
Silver:  /mnt/pei/silver/{customers_enriched, products_enriched, orders_enriched}
Gold:    /mnt/pei/gold/{profit_by_year, profit_by_year_category, profit_by_customer, profit_by_customer_year}

Project Contents

mnt/pei/source/        Actual data files (793 customers, 9,994 orders, 1,862 products)
schemas/               Schema definitions for Bronze/Silver/Gold layers
src/                   Core transformation logic (TDD approach)
tests/                 Unit tests 
notebooks/             4 executable notebooks

Key Features

- TDD approach with comprehensive tests
- Schema-driven development with explicit schema usage
- Profit rounded to 2 decimal places (CRITICAL requirement)
- Customer name and country in enriched orders
- Product category and sub-category in enriched orders
- All PySpark transformations (no SQL in transformations)
- SQL queries for final analysis
- Simple /mnt/pei paths (no os.path)

01_bronze_ingestion.py:
- Loads Customer.xlsx, Orders.json, Products.csv using schemas
- Parses dates from JSON
- Saves to /mnt/pei/bronze/

02_silver_enrichment.py:
- Enriches customers (email validation)
- Enriches products (price tiers)
- Enriches orders:
  - Joins with customers and products
  - Adds customer name and country
  - Adds product category and sub-category
  - Rounds profit to 2 decimals
- Saves to /mnt/pei/silver/

03_gold_aggregation.py:
- Creates 4 aggregate tables
- Saves to /mnt/pei/gold/

04_sql_analytics.py:
- Executes SQL queries on aggregates
- Displays results
