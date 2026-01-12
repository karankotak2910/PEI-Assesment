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
