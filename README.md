# PEI-Assesment
Senior Data Engineer Task

Scenario: E-commerce Sales Data Processing with Databricks
You've been assigned to design and implement a data processing system using Databricks for an e-commerce platform. This platform generates a lot of sales data, including details about orders, products, customers, and transactions. Your goal is to use Databricks to create a scalable, efficient, and reliable data engineering solution. This solution should process and analyze the data to provide valuable insights for business stakeholders.

Source Datasets:
You are required to download the datasets from the below drive
https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing

Data Transformation and Processing:
Your task is to process the raw sales data using Databricks notebooks and PySpark. You need to clean up the data and transform it into structured formats suitable for analysis. Specifically, you should create a master table and perform aggregations based on the requirements provided.
Note: Write appropriate Test Cases (Unit Tests) to ensure the correctness for the given scenarios. Use PySpark (not SQL) for this task. 			
Task
1. Create raw tables for each source dataset
2. Create an enriched table for customers and products
3. Create an enriched table which has
    1. order information
        1. Profit rounded to 2 decimal places
    2. Customer name and country
    3. Product category and sub category
4. Create an aggregate table that shows profit by
    1. Year
    2. Product Category
    3. Product Sub Category
    4. Customer
5. Using SQL output the following aggregates
    1. Profit by Year
    2. Profit by Year + Product Category
    3. Profit by Customer
    4. Profit by Customer + Year
Notes:
* Ensure you understand the task requirements thoroughly before starting.
* Pay attention to specific details and expectations outlined in the task descriptions.
* Use a test-driven development approach to validate the correctness of your implementations.
* Write comprehensive test cases to cover different scenarios and edge cases.
* Ensure your solution handles data quality issues and implements robust error-handling mechanisms.
* Document your code and assumptions clearly to make it understandable for others.
* Consider performance implications and optimize your code for efficiency and scalability.


######################################

# PEI Assessment - E-commerce Sales Data Processing

### Unity Catalog Structure

```
workspace (catalog)
├── bronze (schema)
│   ├── customers
│   ├── products
│   └── orders
├── silver (schema)
│   ├── customers_enriched
│   ├── products_enriched
│   └── orders_enriched (partitioned by order_year, order_month)
└── gold (schema)
    └── profit_aggregate
```

### Data Flow

```
Source Files → Bronze Layer → Silver Layer → Gold Layer → SQL Analytics
   (CSV/JSON/XLSX)   (Raw Tables)   (Enriched)    (Aggregates)    (Business Insights)
```

## Project Structure

```
pei_assessment/
├── source_data/              # Source files (place data here)
│   ├── Customer.xlsx         # 793 customers
│   ├── Orders.json           # 9,994 orders
│   └── Products.csv          # 1,862 products
├── schemas/                  # PySpark schema definitions
│   ├── bronze_schema.py      # Raw data schemas
│   ├── silver_schema.py      # Enriched data schemas
│   └── gold_schema.py        # Aggregate schemas
├── src/                      # Core business logic
│   ├── data_loader.py        # Unity Catalog data I/O functions
│   ├── transformations.py    # PySpark transformations
│   └── data_quality.py       # Comprehensive DQ validation
├── tests/                    # Unit tests (pytest)
│   ├── conftest.py           # Test fixtures and shared setup
│   ├── test_integration.py   # Integration logic tests
│   ├── test_merge_operations.py  # Merge logic tests
|   ├── test_transformations.py  # Transformation logic tests
│   └── test_data_quality.py     # DQ validation tests
├── notebooks/                # Pipeline execution notebooks
│   ├── 01_bronze_ingestion.py   # Load source → Bronze tables
│   ├── 02_silver_enrichment.py  # Bronze → Silver enrichment
│   ├── 03_gold_aggregation.py   # Silver → Gold aggregates
│   └── 04_sql_analytics.py      # SQL business intelligence queries
│   └── 05_data_quality_validation.py      # DQ functions
├── QUICKSTART.md            # Quick start guide
└── TESTING.md               # Testing guide
```

## Unity Catalog Tables

### Bronze Layer (workspace.bronze.*)
Raw, unprocessed data loaded from source files.

| Table | Description | Records |
|-------|-------------|---------|
| `workspace.bronze.customers` | Customer master data | 793 |
| `workspace.bronze.products` | Product catalog | 1,862 |
| `workspace.bronze.orders` | Order transactions (dates parsed) | 9,994 |

### Silver Layer (workspace.silver.*)
Cleansed, enriched, and business-ready data.

| Table | Description | Key Enrichments |
|-------|-------------|-----------------|
| `workspace.silver.customers_enriched` | Enriched customers | Email validation, domain extraction |
| `workspace.silver.products_enriched` | Enriched products | Standardized columns |
| `workspace.silver.orders_enriched` | Enriched orders (partitioned) | Customer name/country, Category/sub-category, **Profit rounded to 2 decimals**, Date hierarchies |

### Gold Layer (workspace.gold.*)
Business-level aggregates for analytics and reporting.

| Table | Description | Dimensions |
|-------|-------------|------------|
| `workspace.gold.profit_aggregate` | Single aggregate table | Year, Category, Sub-Category, Customer, Profit, Sales, Order Count |

## Assessment Requirements Mapping

### Task 1: Create raw tables for each source dataset 
- Implementation: `01_bronze_ingestion.py`
- **Output:** `workspace.bronze.{customers, products, orders}`
- **Function:** `save_bronze_table()`

### Task 2: Create enriched table for customers and products 
- **Implementation:** `src/transformations.py` → `enrich_customer()`, `enrich_product()`
- **Output:** `workspace.silver.{customers_enriched, products_enriched}`
- **CustomerEnrichments:** Email domain extraction, email validation
- **Product Enrichments:** Column standardization

### Task 3: Create enriched order table with 
- Order information
- **Profit rounded to 2 decimal places (CRITICAL)**
- Customer name and country
- Product category and sub-category

**Implementation:** `src/transformations.py` → `enrich_order()`  
**Output:** `workspace.silver.orders_enriched`  
**Key Function:** `round_profit_to_2_decimals()` using `Decimal(10,2)`

### Task 4: Create aggregate table showing profit by 
- Year
- Product Category
- Product Sub Category
- Customer

**Implementation:** `src/transformations.py` → `create_profit_aggregate()`  
**Output:** `workspace.gold.profit_aggregate` (single table with all dimensions)

### Task 5: Using SQL output the following aggregates 
- Profit by Year
- Profit by Year + Product Category
- Profit by Customer
- Profit by Customer + Year

**Implementation:** `04_sql_analytics.py`  
**Method:** SQL queries on `workspace.gold.profit_aggregate` with different GROUP BY clauses
