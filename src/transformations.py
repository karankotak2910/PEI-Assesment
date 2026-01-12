"""Data transformation functions following TDD approach"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, year, month, quarter, datediff, round as spark_round,
    regexp_extract, sum as spark_sum, count
)


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
        .withColumnRenamed("State", "product_state")


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
        customers_df.select("customer_id", "customer_name", "country", "segment", "region"),
        on="customer_id",
        how="left"
    )


def join_with_product(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Join orders with products to add product information.
    
    Adds category and sub_category as per requirement.
    Joins on product_id only (state is product-specific, not a join key).
    
    Args:
        orders_df: Orders DataFrame with product_id
        products_df: Enriched products DataFrame
        
    Returns:
        DataFrame with product information joined
    """
    return orders_df.join(
        products_df.select("product_id", "product_name", "category", "sub_category", "price_per_unit"),
        on="product_id",
        how="left"
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
    2. Join with customers (adds customer_name, country)
    3. Join with products (adds category, sub_category)
    4. Add date hierarchies
    5. Calculate days to ship
    6. Round profit to 2 decimals
    
    Args:
        orders_df: Bronze orders DataFrame
        customers_df: Enriched customers DataFrame
        products_df: Enriched products DataFrame
        
    Returns:
        Enriched orders DataFrame with all requirements met
    """
    df = standardize_order_columns(orders_df)
    df = join_with_customer(df, customers_df)
    df = join_with_product(df, products_df)
    df = add_date_fields(df)
    df = calculate_days_to_ship(df)
    df = round_profit_to_2_decimals(df)
    
    final_cols = [
        "order_id", "order_date", "ship_date", "ship_mode",
        "order_year", "order_month", "order_quarter",
        "customer_id", "customer_name", "country", "segment", "region",
        "product_id", "product_name", "category", "sub_category",
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
        spark_round(spark_sum("price"), 2).cast("decimal(15,2)").alias("total_sales"),
        count("order_id").alias("order_count")
    ).withColumnRenamed("order_year", "year") \
     .orderBy("year", "category", "sub_category", col("total_profit").desc())
