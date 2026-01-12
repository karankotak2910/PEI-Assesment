"""Data loading functions"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, split, lpad, concat, lit, current_timestamp
import pandas as pd
from schemas.bronze_schema import CUSTOMER_BRONZE, PRODUCT_BRONZE, ORDER_BRONZE


SOURCE_PATH = "source_data"

# Unity Catalog namespace
CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


def get_spark_session(app_name="PEI_Assessment"):
    """
    Create SparkSession with Delta Lake and Unity Catalog support.
    
    Creates catalog 'workspace' with schemas: bronze, silver, gold
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession configured for Delta Lake and Unity Catalog
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Create Unity Catalog structure if not exists
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"USE CATALOG {CATALOG_NAME}")
    
    # Create schemas for bronze, silver, gold layers
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    
    return spark


def load_customer_data(spark: SparkSession, file_path: str = None) -> DataFrame:
    """
    Load customer data from Excel file.
    
    Args:
        spark: SparkSession
        file_path: Path to Customer.xlsx file
        
    Returns:
        DataFrame with bronze customer schema
    """
    if file_path is None:
        file_path = f"{SOURCE_PATH}/Customer.xlsx"
    
    pdf = pd.read_excel(file_path)
    return spark.createDataFrame(pdf, schema=CUSTOMER_BRONZE)


def load_product_data(spark: SparkSession, file_path: str = None) -> DataFrame:
    """
    Load product data from CSV file.
    
    Args:
        spark: SparkSession
        file_path: Path to Products.csv file
        
    Returns:
        DataFrame with bronze product schema
    """
    if file_path is None:
        file_path = f"{SOURCE_PATH}/Products.csv"
    
    return spark.read.schema(PRODUCT_BRONZE).option("header", "true").csv(file_path)


def load_order_data(spark: SparkSession, file_path: str = None) -> DataFrame:
    """
    Load order data from JSON file and parse dates.
    
    The JSON contains dates in 'd/M/yyyy' or 'dd/MM/yyyy' format (e.g., '21/8/2016' or '01/12/2016').
    This function converts them to proper DateType by:
    1. Splitting the date string by '/'
    2. Padding day and month with leading zeros
    3. Concatenating in dd-MM-yyyy format
    4. Parsing to DateType
    
    Args:
        spark: SparkSession
        file_path: Path to Orders.json file
        
    Returns:
        DataFrame with bronze order schema and parsed dates
    """
    if file_path is None:
        file_path = f"{SOURCE_PATH}/Orders.json"
    
    df = spark.read.schema(ORDER_BRONZE).option("multiLine", "true").json(file_path)
    
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


def save_bronze_table(spark: SparkSession, df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    Save DataFrame as Delta table in Bronze schema.
    Bronze layer uses OVERWRITE mode (truncate and load).
    
    Table: workspace.bronze.{table_name}
    
    Args:
        spark: SparkSession
        df: DataFrame to save
        table_name: Name of the table (e.g., 'customers')
        mode: Write mode (default: overwrite for truncate and load)
    """
    full_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"
    df.write.format("delta").mode(mode).saveAsTable(full_table_name)
    print(f"Saved Bronze table (truncate and load): {full_table_name}")


def merge_silver_customers(spark: SparkSession, df: DataFrame, table_name: str = "customers_enriched"):
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
           AND target.customer_name = source.customer_name
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def merge_silver_products(spark: SparkSession, df: DataFrame, table_name: str = "products_enriched"):
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
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def merge_silver_orders(spark: SparkSession, df: DataFrame, table_name: str = "orders_enriched"):
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
        ON target.customer_id = source.customer_id 
           AND target.order_id = source.order_id 
           AND target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"Merged into Silver table: {full_table_name}")


def save_silver_table(spark: SparkSession, df: DataFrame, table_name: str, partition_cols: list = None, mode: str = "overwrite"):
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


def save_gold_table(spark: SparkSession, df: DataFrame, table_name: str, partition_cols: list = None, mode: str = "overwrite"):
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


def load_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Load Delta table from Bronze schema.
    
    Args:
        spark: SparkSession
        table_name: Name of the table to load
        
    Returns:
        DataFrame from workspace.bronze.{table_name}
    """
    full_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"
    return spark.table(full_table_name)


def load_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
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


def load_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
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
