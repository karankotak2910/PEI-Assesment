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
    StructField("product_state", StringType(), True)
])

ORDER_ENRICHED = StructType([
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
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("discount", DoubleType(), True),
    StructField("price", DoubleType(), False),
    StructField("price_per_unit", DoubleType(), False),
    StructField("profit", DecimalType(10, 2), False),
    StructField("days_to_ship", IntegerType(), True)
])
