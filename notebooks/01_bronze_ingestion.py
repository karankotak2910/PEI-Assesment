"""
Bronze Layer - Raw Data Ingestion
Load Customer.xlsx, Orders.json, Products.csv and save to Unity Catalog Bronze schema
"""

import sys
from src.data_loader import get_spark_session, load_customer_data, load_product_data, load_order_data, save_bronze_table
from src.data_quality import validate_schema
from schemas.bronze_schema import CUSTOMER_BRONZE, PRODUCT_BRONZE, ORDER_BRONZE


def main():
    spark = get_spark_session("Bronze_Ingestion")
    
    try:
        print("Loading source data files...")
        customers = load_customer_data(spark)
        products = load_product_data(spark)
        orders = load_order_data(spark)
        
        print(f"Customers: {customers.count():,} records")
        print(f"Products: {products.count():,} records")
        print(f"Orders: {orders.count():,} records")
        
        # Validate schemas match expected bronze schemas
        print("\nValidating schemas against bronze schema definitions...")
        customer_cols = [field.name for field in CUSTOMER_BRONZE.fields]
        product_cols = [field.name for field in PRODUCT_BRONZE.fields]
        order_cols = [field.name for field in ORDER_BRONZE.fields]
        
        validate_schema(customers, customer_cols, "bronze.customers")
        print("Customer schema validated")
        
        validate_schema(products, product_cols, "bronze.products")
        print("Product schema validated")
        
        validate_schema(orders, order_cols, "bronze.orders")
        print("Order schema validated")
        
        print("\nSaving to Bronze schema in Unity Catalog...")
        save_bronze_table(spark, customers, "customers")
        save_bronze_table(spark, products, "products")
        save_bronze_table(spark, orders, "orders")
        
        print("\nBronze tables created successfully")
        print("Unity Catalog: workspace.bronze.*")
        
        print("\nSample Data:")
        print("\nCustomers:")
        customers.select("Customer ID", "Customer Name", "Segment", "Country").show(5, truncate=False)
        
        print("\nProducts:")
        products.select("Product ID", "Category", "Sub-Category", "Product Name").show(5, truncate=False)
        
        print("\nOrders:")
        orders.select("Order ID", "Order Date", "Customer ID", "Product ID", "Profit").show(5, truncate=False)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
