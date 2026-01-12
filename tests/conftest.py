"""Pytest configuration and test fixtures"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("PEI_Testing") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def bronze_customers(spark):
    schema = StructType([
        StructField("Customer ID", StringType(), False),
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
    
    data = [
        ("PW-19240", "Pierre Wener", "pierre@example.com", "123-456-7890", 
         "123 Main St", "Consumer", "United States", "Louisville", "Colorado", "80027", "West"),
        ("GH-14410", "Gary Hansen", "gary@test.com", "987-654-3210",
         "456 Oak Ave", "Home Office", "United States", "Chicago", "Illinois", "60653", "Central"),
        ("KL-16555", "Kelly Lampkin", None, "555-123-4567",
         "789 Pine Rd", "Corporate", "United States", "Denver", "Colorado", "80906", "West"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def bronze_products(spark):
    schema = StructType([
        StructField("Product ID", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Sub-Category", StringType(), False),
        StructField("Product Name", StringType(), False),
        StructField("State", StringType(), True),
        StructField("Price per product", DoubleType(), False)
    ])
    
    data = [
        ("FUR-CH-10002961", "Furniture", "Chairs", "Leather Task Chair", "New York", 81.88),
        ("TEC-AC-10004659", "Technology", "Accessories", "Flash Drive 16GB", "Oklahoma", 72.99),
        ("OFF-BI-10002824", "Office Supplies", "Binders", "Easel Ring Binders", "Colorado", 4.25),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def bronze_orders(spark):
    schema = StructType([
        StructField("Row ID", IntegerType(), False),
        StructField("Order ID", StringType(), False),
        StructField("Order Date", DateType(), False),
        StructField("Ship Date", DateType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), False),
        StructField("Product ID", StringType(), False),
        StructField("Quantity", IntegerType(), False),
        StructField("Price", DoubleType(), False),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])
    
    data = [
        (1, "CA-2016-122581", date(2016, 8, 21), date(2016, 8, 25), "Standard Class",
         "PW-19240", "FUR-CH-10002961", 7, 573.17, 0.3, 63.69),
        (2, "CA-2016-138478", date(2016, 9, 15), date(2016, 9, 20), "Second Class",
         "GH-14410", "TEC-AC-10004659", 2, 145.98, 0.0, 186.45),
        (3, "US-2017-110526", date(2017, 3, 10), None, "First Class",
         "KL-16555", "OFF-BI-10002824", 5, 21.25, 0.1, -93.54),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def enriched_customers(bronze_customers):
    from src.transformations import enrich_customer
    return enrich_customer(bronze_customers)


@pytest.fixture
def enriched_products(bronze_products):
    from src.transformations import enrich_product
    return enrich_product(bronze_products)
