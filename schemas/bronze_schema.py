"""Bronze Layer Schemas"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CUSTOMER_BRONZE = StructType([
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
