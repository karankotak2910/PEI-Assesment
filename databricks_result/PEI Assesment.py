# Databricks notebook source
# DBTITLE 1,BRONZE SCHEMA
"""Bronze Layer Schemas"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CUSTOMER_BRONZE = StructType([
    StructField("Customer ID1111", IntegerType(), False),
    StructField("Customer Name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True)
])

PRODUCT_BRONZE = StructType([
    StructField("Product ID", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("Sub-Category", StringType(), False),
    StructField("Product Name", StringType(), False),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), False)
])

ORDER_BRONZE = StructType([
    StructField("Row ID", IntegerType(), False),
    StructField("Order ID", StringType(), False),
    StructField("Order Date", StringType(), False),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), False),
    StructField("Product ID", StringType(), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Price", DoubleType(), False),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])


# COMMAND ----------

# DBTITLE 1,SILVER SCHEMA
"""Silver Layer Schemas - Enriched data"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, DecimalType

CUSTOMER_ENRICHED = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("segment", StringType(), False),
    StructField("country", StringType(), False),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("email_domain", StringType(), True),
    StructField("is_valid_email", StringType(), True)
])

PRODUCT_ENRICHED = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("price_per_unit", DoubleType(), False),
    StructField("state", StringType(), True)
])

ORDER_ENRICHED = StructType([
    StructField("row_id", IntegerType(), False),
    StructField("order_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("ship_date", DateType(), True),
    StructField("ship_mode", StringType(), True),
    StructField("order_year", IntegerType(), False),
    StructField("order_month", IntegerType(), False),
    StructField("order_quarter", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("segment", StringType(), True),
    StructField("region", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("discount", DoubleType(), True),
    StructField("price", DoubleType(), False),
    StructField("price_per_unit", DoubleType(), False),
    StructField("profit", DecimalType(10, 2), False),
    StructField("days_to_ship", IntegerType(), True)
])


# COMMAND ----------

"""Gold Layer Schemas - Business aggregates"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

PROFIT_AGGREGATE = StructType([
    StructField("year", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("total_profit", DecimalType(15, 2), False),
    StructField("total_sales", DecimalType(15, 2), False),
    StructField("order_count", IntegerType(), False)
])


# COMMAND ----------

### ASSIGNING SCHEMA WON't WORK HERE AS DELTA TABLES ARE ALREADY CREATED AND WILL BE USING IT'S OWN METADATA - COMMUNITY EDITION IS NOT ALLOWING THE USAGE OF DBFS, INSTEAD TABLES ARE CREATED ### 
from pyspark.sql.functions import to_date, regexp_replace, split, lpad, lit, concat

customer_bronze = spark.read.format('delta').schema(CUSTOMER_BRONZE).table("workspace.bronze.customer")

order_bronze = spark.read.format('delta').schema(ORDER_BRONZE).table("workspace.test.orders")
product_bronze = spark.read.format('delta').schema(PRODUCT_BRONZE).table("workspace.bronze.products")

order_bronze = order_bronze.withColumn("date_parts", split("Order Date", "/"))
order_bronze = order_bronze.withColumn(
        "Order Date",
        to_date(
            concat(
                lpad(order_bronze.date_parts[0], 2, '0'),  # day padded to 2 digits
                lit("-"),
                lpad(order_bronze.date_parts[1], 2, '0'),  # month padded to 2 digits
                lit("-"),
                order_bronze.date_parts[2]                  # year
            ),
            "dd-MM-yyyy"
        )
    )
    
# Parse Ship Date (same logic)
order_bronze = order_bronze.withColumn("ship_date_parts", split("Ship Date", "/"))
order_bronze = order_bronze.withColumn(
        "Ship Date",
        to_date(
            concat(
                lpad(order_bronze.ship_date_parts[0], 2, '0'),  # day padded
                lit("-"),
                lpad(order_bronze.ship_date_parts[1], 2, '0'),  # month padded
                lit("-"),
                order_bronze.ship_date_parts[2]                  # year
            ),
            "dd-MM-yyyy"
        )
    )
order_bronze.createOrReplaceTempView('order_bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select `CUSTOMER ID`, `CUSTOMER NAME` from workspace.bronze.customer
# MAGIC -- GROUP BY `CUSTOMER ID`, `CUSTOMER NAME`
# MAGIC -- HAVING COUNT(*)>1 --- KEY Coliumns
# MAGIC
# MAGIC select distinct * from workspace.bronze.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from workspace.test.orders
# MAGIC
# MAGIC -- select `customer id`, `product id`, `order id` from workspace.test.orders
# MAGIC -- group by `customer id`, `product id`, `order id`
# MAGIC -- having count(*) > 1
# MAGIC
# MAGIC select * from workspace.test.orders where `customer id`='LC-16870' and `product id`='TEC-AC-10002006' and `order id`='CA-2017-118017'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select `PRODUCT Id`--, `state` --,`Product Name`
# MAGIC -- from workspace.bronze.products
# MAGIC -- group by `PRODUCT Id`--, `state` --,`Product Name`
# MAGIC -- having count(*)>1
# MAGIC -- select `product id` from workspace.bronze.orders
# MAGIC -- intersect
# MAGIC -- select `product id` from workspace.bronze.products where `Product id` in ('FUR-CH-10002961', 'OFF-PA-10001970','OFF-EN-10003134','FUR-FU-10003981')
# MAGIC
# MAGIC select `product id`, category, `sub-category`, `price per product` from 
# MAGIC workspace.bronze.products
# MAGIC group by all
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select year(cast(`order date` as date)), substr(`order date` as sum(profit) from workspace.bronze.orders
# MAGIC -- group by year(try_cast(`order date` as date))
# MAGIC
# MAGIC -- SELECT `row id`--, `Customer ID`,`order id`, `product id` 
# MAGIC -- FROM order_bronze 
# MAGIC -- group by `row id`--,`Customer ID`, `order id`, `product id`
# MAGIC -- having count(*)>1
# MAGIC
# MAGIC -- select * from workspace.bronze.orders
# MAGIC -- where `Customer ID`='LC-16870' and `order id`='CA-2017-118017' and `product id`='TEC-AC-10002006'
# MAGIC
# MAGIC
# MAGIC select p.*, c.state, o.*, c.* from workspace.bronze.orders o
# MAGIC join workspace.bronze.customer c on c.`customer id` = o.`customer id`
# MAGIC left join workspace.bronze.products p on p.`product id` = o.`product id` --and c.`state`=p.`state`
# MAGIC where p.`product id` = 'TEC-AC-10003832'
# MAGIC --p.state is null-- c.state
# MAGIC --order by o.`Customer ID`
# MAGIC
# MAGIC --where `Customer ID` = 'FUR-CH-10002961'

# COMMAND ----------

# DBTITLE 1,DATA LOADING
from pyspark.sql.functions import current_timestamp

def merge_silver_customers(spark, df: DataFrame, table_name: str = "customers_enriched"):
    """
    Merge customers into Silver table using MERGE operation.
    
    Merge Keys: customer_id, customer_name
    Strategy: UPDATE when matched, INSERT when not matched
    
    Args:
        spark: SparkSession
        df: New/updated customer DataFrame
        table_name: Name of the Silver table
    """
    full_table_name = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    # Add metadata columns
    df = df.withColumn("updated_at", current_timestamp())
    
    # Check if table exists
    table_exists = spark.catalog.tableExists(full_table_name)
    
    if not table_exists:
        # First load - create table
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"Created Silver table: {full_table_name}")
    else:
        # Merge operation
        df.createOrReplaceTempView("customers_updates")
        
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING customers_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def merge_silver_products(spark, df: DataFrame, table_name: str = "products_enriched"):
    """
    Merge products into Silver table using MERGE operation.
    
    Merge Keys: product_id, product_name
    Strategy: UPDATE when matched, INSERT when not matched
    
    Args:
        spark: SparkSession
        df: New/updated product DataFrame
        table_name: Name of the Silver table
    """
    full_table_name = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    # Add metadata columns
    df = df.withColumn("updated_at", current_timestamp())
    
    # Check if table exists
    table_exists = spark.catalog.tableExists(full_table_name)
    
    if not table_exists:
        # First load - create table
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"Created Silver table: {full_table_name}")
    else:
        # Merge operation
        df.createOrReplaceTempView("products_updates")
        
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING products_updates AS source
        ON target.product_id = source.product_id 
           AND target.product_name = source.product_name
           AND target.state = source.state
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def merge_silver_orders(spark, df: DataFrame, table_name: str = "orders_enriched"):
    """
    Merge orders into Silver table using MERGE operation.
    
    Merge Keys: customer_id, order_id, product_id
    Strategy: UPDATE when matched, INSERT when not matched
    Partitioned by: order_year, order_month
    
    Args:
        spark: SparkSession
        df: New/updated orders DataFrame
        table_name: Name of the Silver table
    """
    full_table_name = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    # Add metadata columns
    df = df.withColumn("updated_at", current_timestamp())
    
    # Check if table exists
    table_exists = spark.catalog.tableExists(full_table_name)
    
    if not table_exists:
        # First load - create table with partitioning
        df.write.format("delta").mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .saveAsTable(full_table_name)
        print(f"Created Silver table (partitioned): {full_table_name}")
    else:
        # Merge operation
        df.createOrReplaceTempView("orders_updates")
        
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING orders_updates AS source
        ON target.row_id = source.row_id
           AND target.customer_id = source.customer_id 
           AND target.order_id = source.order_id 
           AND target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def save_silver_table(spark, df: DataFrame, table_name: str, partition_cols: list = None, mode: str = "overwrite"):
    """
    DEPRECATED: Use merge_silver_customers, merge_silver_products, or merge_silver_orders instead.
    
    This function is kept for backward compatibility but should not be used for new code.
    Silver layer should use MERGE operations, not overwrite.
    """
    full_table_name = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    if partition_cols:
        df.write.format("delta").mode(mode).partitionBy(partition_cols).saveAsTable(full_table_name)
    else:
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)
    
    print(f"WARNING: Using overwrite mode for Silver table: {full_table_name}")
    print(f"Consider using merge_silver_* functions instead for incremental updates")


def save_gold_table(spark, df: DataFrame, table_name: str, partition_cols: list = None, mode: str = "overwrite"):
    """
    Save DataFrame as Delta table in Gold schema.
    
    Table: workspace.gold.{table_name}
    
    Args:
        spark: SparkSession
        df: DataFrame to save
        table_name: Name of the table (e.g., 'profit_aggregate')
        partition_cols: Optional list of columns to partition by
        mode: Write mode (default: overwrite)
    """
    full_table_name = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{table_name}"
    
    if partition_cols:
        df.write.format("delta").mode(mode).partitionBy(partition_cols).saveAsTable(full_table_name)
    else:
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)
    
    print(f"Saved Gold table: {full_table_name}")


def load_bronze_table(spark, table_name: str) -> DataFrame:
    """
    Load Delta table from Bronze schema.
    
    Args:
        spark: SparkSession
        table_name: Name of the table to load
        
    Returns:
        DataFrame from workspace.bronze.{table_name}
    """
    full_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"

    df = spark.table(full_table_name)
    df_dedup = df.dropDuplicates()
    df_dedup.count()

    return df_dedup


def load_silver_table(spark, table_name: str) -> DataFrame:
    """
    Load Delta table from Silver schema.
    
    Args:
        spark: SparkSession
        table_name: Name of the table to load
        
    Returns:
        DataFrame from workspace.silver.{table_name}
    """
    full_table_name = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    return spark.table(full_table_name)


def load_gold_table(spark, table_name: str) -> DataFrame:
    """
    Load Delta table from Gold schema.
    
    Args:
        spark: SparkSession
        table_name: Name of the table to load
        
    Returns:
        DataFrame from workspace.gold.{table_name}
    """
    full_table_name = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{table_name}"
    return spark.table(full_table_name)


# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
"""Data transformation functions following TDD approach"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, year, month, quarter, datediff, round as spark_round,
    regexp_extract, sum as spark_sum, count
)

CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

def date_transform(df: DataFrame) -> DataFrame:
    """
    Convert date fields from string to date
    """
    # Parse Order Date
    # Example: '21/8/2016' -> ['21', '8', '2016'] -> '21-08-2016'
    df = df.withColumn("date_parts", split("Order Date", "/"))
    df = df.withColumn(
        "Order Date",
        to_date(
            concat(
                lpad(df.date_parts[0], 2, '0'),  # day padded to 2 digits
                lit("-"),
                lpad(df.date_parts[1], 2, '0'),  # month padded to 2 digits
                lit("-"),
                df.date_parts[2]                  # year
            ),
            "dd-MM-yyyy"
        )
    )
    
    # Parse Ship Date (same logic)
    df = df.withColumn("ship_date_parts", split("Ship Date", "/"))
    df = df.withColumn(
        "Ship Date",
        to_date(
            concat(
                lpad(df.ship_date_parts[0], 2, '0'),  # day padded
                lit("-"),
                lpad(df.ship_date_parts[1], 2, '0'),  # month padded
                lit("-"),
                df.ship_date_parts[2]                  # year
            ),
            "dd-MM-yyyy"
        )
    )
    
    # Drop temporary columns
    df = df.drop("date_parts", "ship_date_parts")
    return df

def standardize_customer_columns(df: DataFrame) -> DataFrame:
    """
    Standardize customer column names from bronze to silver layer.
    Converts source column names to lowercase with underscores.
    
    Args:
        df: Bronze customer DataFrame with source column names
        
    Returns:
        DataFrame with standardized column names
    """
    return df \
        .withColumnRenamed("Customer ID", "customer_id") \
        .withColumnRenamed("Customer Name", "customer_name") \
        .withColumnRenamed("Segment", "segment") \
        .withColumnRenamed("Country", "country") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Postal Code", "postal_code") \
        .withColumnRenamed("Region", "region")


def add_email_enrichment(df: DataFrame) -> DataFrame:
    """
    Add email domain extraction and validation fields.
    
    Enrichment logic:
    - email_domain: Extracts domain from email address
    - is_valid_email: Validates email format using regex pattern
    
    Args:
        df: DataFrame with email column
        
    Returns:
        DataFrame with email_domain and is_valid_email columns
    """
    df = df.withColumn(
        "email_domain",
        when(col("email").isNotNull(), regexp_extract(col("email"), r"@(.+)$", 1)).otherwise(None)
    )
    
    df = df.withColumn(
        "is_valid_email",
        when(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"), "Yes").otherwise("No")
    )
    
    return df


def enrich_customer(df: DataFrame) -> DataFrame:
    """
    Complete customer enrichment: Bronze to Silver transformation.
    
    Transformations:
    1. Standardize column names
    2. Add email domain extraction and validation
    
    Args:
        df: Bronze customer DataFrame
        
    Returns:
        Enriched customer DataFrame
    """
    df = standardize_customer_columns(df)
    df = add_email_enrichment(df)
    return df


def standardize_product_columns(df: DataFrame) -> DataFrame:
    """
    Standardize product column names from bronze to silver layer.
    
    Args:
        df: Bronze product DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    return df \
        .withColumnRenamed("Product ID", "product_id") \
        .withColumnRenamed("Product Name", "product_name") \
        .withColumnRenamed("Category", "category") \
        .withColumnRenamed("Sub-Category", "sub_category") \
        .withColumnRenamed("Price per product", "price_per_unit") \
        .withColumnRenamed("State", "state")


def enrich_product(df: DataFrame) -> DataFrame:
    """
    Complete product enrichment: Bronze to Silver transformation.
    
    Transformations:
    1. Standardize column names
    
    Args:
        df: Bronze product DataFrame
        
    Returns:
        Enriched product DataFrame
    """
    return standardize_product_columns(df)


def standardize_order_columns(df: DataFrame) -> DataFrame:
    """
    Standardize order column names from bronze to silver layer.
    Renames Profit to source_profit to track original value.
    
    Args:
        df: Bronze order DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    return df \
        .withColumnRenamed("Row ID", "row_id") \
        .withColumnRenamed("Order ID", "order_id") \
        .withColumnRenamed("Order Date", "order_date") \
        .withColumnRenamed("Ship Date", "ship_date") \
        .withColumnRenamed("Ship Mode", "ship_mode") \
        .withColumnRenamed("Customer ID", "customer_id") \
        .withColumnRenamed("Product ID", "product_id") \
        .withColumnRenamed("Quantity", "quantity") \
        .withColumnRenamed("Price", "price") \
        .withColumnRenamed("Discount", "discount") \
        .withColumnRenamed("Profit", "source_profit")


def add_date_fields(df: DataFrame) -> DataFrame:
    """
    Extract date hierarchies from order_date.
    
    Derived fields:
    - order_year: Year from order date
    - order_month: Month from order date
    - order_quarter: Quarter from order date
    
    Args:
        df: DataFrame with order_date column
        
    Returns:
        DataFrame with date hierarchy columns
    """
    return df \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_quarter", quarter(col("order_date")))


def calculate_days_to_ship(df: DataFrame) -> DataFrame:
    """
    Calculate days between order date and ship date.
    Returns None if ship_date is null.
    
    Args:
        df: DataFrame with order_date and ship_date columns
        
    Returns:
        DataFrame with days_to_ship column
    """
    return df.withColumn(
        "days_to_ship",
        when(col("ship_date").isNotNull(), datediff(col("ship_date"), col("order_date"))).otherwise(None)
    )


def round_profit_to_2_decimals(df: DataFrame) -> DataFrame:
    """
    Round profit to 2 decimal places as per requirement.
    
    CRITICAL REQUIREMENT: Profit must be rounded to exactly 2 decimal places.
    Casts to Decimal(10,2) for precision.
    
    Args:
        df: DataFrame with source_profit column
        
    Returns:
        DataFrame with profit column rounded to 2 decimals
    """
    return df.withColumn("profit", spark_round(col("source_profit"), 2).cast("decimal(10,2)"))


def join_with_customer(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    Join orders with customers to add customer information.
    
    Adds customer_name and country as per requirement.
    
    Args:
        orders_df: Orders DataFrame with customer_id
        customers_df: Enriched customers DataFrame
        
    Returns:
        DataFrame with customer information joined
    """
    return orders_df.join(
        customers_df.select("customer_id", "customer_name", "country", "segment", "region", "state"),
        on="customer_id",
        how="inner"
    )


def join_with_product(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Join orders with products to add product information.
    
    Adds category and sub_category as per requirement.
    
    Args:
        orders_df: Orders DataFrame with product_id
        products_df: Enriched products DataFrame
        
    Returns:
        DataFrame with product information joined
    """
    products_df = products_df.select("product_id", "category", "sub_category", "price_per_unit").dropDuplicates()
    products_df.count()
    return orders_df.join(
        products_df,
        on=["product_id"],
        how="inner"
    )


def enrich_order(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Complete order enrichment: Bronze to Silver transformation.
    
    REQUIREMENT COMPLIANCE:
    1. Order information with profit rounded to 2 decimal places
    2. Customer name and country
    3. Product category and sub-category
    
    Transformations:
    1. Standardize column names
    2. Convert date field from string to date type
    3. Join with customers (adds customer_name, country)
    4. Join with products (adds category, sub_category)
    5. Add date hierarchies
    6. Calculate days to ship
    7. Round profit to 2 decimals
    
    Args:
        orders_df: Bronze orders DataFrame
        customers_df: Enriched customers DataFrame
        products_df: Enriched products DataFrame
        
    Returns:
        Enriched orders DataFrame with all requirements met
    """
    df = date_transform(orders_df)
    df = standardize_order_columns(df)
    df = join_with_customer(df, customers_df).dropDuplicates()
    df = join_with_product(df, products_df).dropDuplicates()
    df = add_date_fields(df)
    df = calculate_days_to_ship(df)
    df = round_profit_to_2_decimals(df)
    
    final_cols = [
        "row_id","order_id", "order_date", "ship_date", "ship_mode",
        "order_year", "order_month", "order_quarter",
        "customer_id", "customer_name", "country", "segment", "region",
        "product_id", "category", "sub_category",
        "quantity", "discount", "price", "price_per_unit",
        "profit", "days_to_ship"
    ]
    
    return df.select(*final_cols)


def create_profit_aggregate(df: DataFrame) -> DataFrame:
    """
    Create single aggregate table with Year, Product Category, Sub Category, and Customer.
    
    REQUIREMENT: Create aggregate table showing profit by:
    1. Year
    2. Product Category
    3. Product Sub Category
    4. Customer
    
    Aggregations:
    - total_profit: Sum of profit rounded to 2 decimals
    - total_sales: Sum of price rounded to 2 decimals
    - order_count: Count of orders
    
    Args:
        df: Enriched orders DataFrame
        
    Returns:
        Aggregate DataFrame grouped by year, category, sub_category, customer
    """
    return df.groupBy(
        "order_year",
        "category", 
        "sub_category",
        "customer_id",
        "customer_name",
        "country"
    ).agg(
        spark_round(spark_sum("profit"), 2).cast("decimal(15,2)").alias("total_profit"),
        spark_round(spark_sum("price"*"quantity"), 2).cast("decimal(15,2)").alias("total_sales"),
        count("order_id").alias("order_count")
    ).withColumnRenamed("order_year", "year") \
     .orderBy("year", "category", "sub_category", col("total_profit").desc())

# COMMAND ----------

# DBTITLE 1,SILVER ENRICHMENT
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    
print("Loading Bronze tables from Unity Catalog (workspace.bronze.*)...")
customers_bronze = load_bronze_table(spark, "customer")
print(f"  Loaded {customers_bronze.count():,} customers")
customers_bronze.createOrReplaceTempView('customers_bronze')
products_bronze = load_bronze_table(spark, "products")
orders_bronze = load_bronze_table(spark, "orders")
        
print("\nEnriching customers...")
customers_enriched = enrich_customer(customers_bronze).dropDuplicates()
print(f"  Enriched {customers_enriched.count():,} customer")
        
print("\nEnriching products...")
products_enriched = enrich_product(products_bronze).dropDuplicates()
print(f"  Enriched {products_enriched.count():,} products")
        
print("\nEnriching orders...")
print("  Joining with customers and products")
print("  Calculating profit (rounded to 2 decimal places)")
print("  Adding customer name and country")
print("  Adding product category and sub-category")
orders_enriched = enrich_order(orders_bronze, customers_enriched, products_enriched).dropDuplicates()
print(f"  Enriched {orders_enriched.count():,} orders")
orders_enriched.createOrReplaceTempView('orders_enriched')
        
print("\nMERGING into Silver schema in Unity Catalog...")
print("  Strategy: UPDATE when matched, INSERT when not matched")
        
# Merge customers: Key = customer_id + customer_name
merge_silver_customers(spark, customers_enriched, "customers_enriched")
        
# Merge products: Key = product_id + product_name
merge_silver_products(spark, products_enriched, "products_enriched")
        
# Merge orders: Key = customer_id + order_id + product_id (partitioned by order_year, order_month)
merge_silver_orders(spark, orders_enriched, "orders_enriched")
        
print("\nSilver tables merged successfully")
print("Unity Catalog: workspace.silver.*")
print("Merge Keys:")
print("  - customers: customer_id, customer_name")
print("  - products: product_id, product_name")
print("  - orders: customer_id, order_id, product_id")
        
print("\nEnriched Orders Sample:")
orders_enriched.select(
            "order_id", "order_date", "customer_name", "country",
            "category", "sub_category", "quantity", "price", "profit"
        ).show(10, truncate=False)
        
print("\nProfit Statistics:")
orders_enriched.selectExpr(
            "count(*) as total_orders",
            "round(sum(profit), 2) as total_profit",
            "round(min(profit), 2) as min_profit",
            "round(max(profit), 2) as max_profit"
        ).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 'bronze_customers', count(*), count(distinct `Customer ID`) from workspace.bronze.customer union
# MAGIC select 'bronze_orders', count(*), count(distinct *) from workspace.bronze.orders union
# MAGIC select 'bronze_products', count(*), count(distinct `Product ID`) from workspace.bronze.products union
# MAGIC select 'silver_customers', count(*), count(distinct *) from workspace.silver.customers_enriched union
# MAGIC select 'silver_orders', count(*), count(distinct *) from workspace.silver.orders_enriched union
# MAGIC select 'silver_products', count(*), count(distinct *) from workspace.silver.products_enriched

# COMMAND ----------

# DBTITLE 1,GOLD AGGREGATE
print("Loading Silver tables from Unity Catalog (workspace.silver.*)...")
orders_enriched = load_silver_table(spark, "orders_enriched")
print(f"  Loaded {orders_enriched.count():,} enriched orders")
        
print("\nCreating aggregate table...")
print("  Aggregating by Year + Category + Sub-Category + Customer...")
        
profit_aggregate = create_profit_aggregate(orders_enriched)
        
print(f"  Created {profit_aggregate.count():,} aggregate records")
        
print("\nSaving to Gold schema in Unity Catalog...")
save_gold_table(spark, profit_aggregate, "profit_aggregate")
        
print("\nGold table created successfully")
print("Unity Catalog: workspace.gold.profit_aggregate")
        
print("\nAggregate Table Sample:")
profit_aggregate.show(20, truncate=False)
        
print("\nAggregate Statistics:")
profit_aggregate.selectExpr(
            "count(*) as total_records",
            "count(distinct year) as distinct_years",
            "count(distinct category) as distinct_categories",
            "count(distinct customer_id) as distinct_customers"
        ).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.profit_aggregate

# COMMAND ----------

print("Loading Gold table from Unity Catalog...")
profit_aggregate = load_gold_table(spark, "profit_aggregate")
        
profit_aggregate.createOrReplaceTempView("profit_aggregate")
        
print(f"  Loaded {profit_aggregate.count():,} aggregate records")
print("  Table: workspace.gold.profit_aggregate")
        
print("\nExecuting SQL queries on aggregate table...")
        
print("\n" + "="*80)
print("Query 1: Profit by Year")
print("="*80)
query1 = """
        SELECT 
            year,
            SUM(total_profit) as total_profit,
            SUM(total_sales) as total_sales,
            SUM(order_count) as order_count
        FROM profit_aggregate
        GROUP BY year
        ORDER BY year
        """
spark.sql(query1).show(truncate=False)
        
print("\n" + "="*80)
print("Query 2: Profit by Year + Product Category")
print("="*80)
query2 = """
        SELECT 
            year,
            category,
            SUM(total_profit) as total_profit,
            SUM(total_sales) as total_sales,
            SUM(order_count) as order_count
        FROM profit_aggregate
        GROUP BY year, category
        ORDER BY year, total_profit DESC
        """
spark.sql(query2).show(truncate=False)
        
print("\n" + "="*80)
print("Query 3: Profit by Customer")
print("="*80)
query3 = """
        SELECT 
            customer_id,
            customer_name,
            country,
            SUM(total_profit) as total_profit,
            SUM(total_sales) as total_sales,
            SUM(order_count) as order_count
        FROM profit_aggregate
        GROUP BY customer_id, customer_name, country
        ORDER BY total_profit DESC
        """
spark.sql(query3).show(20, truncate=False)

print("\n" + "="*80)
print("Query 4: Profit by Customer + Year")
print("="*80)
query4 = """
        SELECT 
            customer_id,
            customer_name,
            country,
            year,
            SUM(total_profit) as total_profit,
            SUM(total_sales) as total_sales,
            SUM(order_count) as order_count
        FROM profit_aggregate
        GROUP BY customer_id, customer_name, country, year
        ORDER BY year, total_profit DESC
        """
spark.sql(query4).show(20, truncate=False)
        
print("\n" + "="*80)
print("Additional Analysis")
print("="*80)
        
print("\nProfit by Year + Category + Sub-Category:")
query5 = """
        SELECT 
            year,
            category,
            sub_category,
            SUM(total_profit) as total_profit,
            SUM(order_count) as order_count
        FROM profit_aggregate
        GROUP BY year, category, sub_category
        ORDER BY total_profit DESC
        """
spark.sql(query5).show(15, truncate=False)
        
print("\nSQL Analytics Complete")