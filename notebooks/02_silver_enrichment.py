"""
Silver Layer - Data Enrichment with MERGE Operations
Load from Bronze schema, enrich, and MERGE into Silver schema in Unity Catalog
"""

import sys
sys.path.insert(0, '.')

from src.data_loader import (
    get_spark_session, load_bronze_table,
    merge_silver_customers, merge_silver_products, merge_silver_orders
)
from src.transformations import enrich_customer, enrich_product, enrich_order


def main():
    spark = get_spark_session("Silver_Enrichment")
    
    try:
        print("Loading Bronze tables from Unity Catalog (workspace.bronze.*)...")
        customers_bronze = load_bronze_table(spark, "customers")
        products_bronze = load_bronze_table(spark, "products")
        orders_bronze = load_bronze_table(spark, "orders")
        
        print("\nEnriching customers...")
        customers_enriched = enrich_customer(customers_bronze)
        print(f"  Enriched {customers_enriched.count():,} customers")
        
        print("\nEnriching products...")
        products_enriched = enrich_product(products_bronze)
        print(f"  Enriched {products_enriched.count():,} products")
        
        print("\nEnriching orders...")
        print("  Joining with customers and products")
        print("  Calculating profit (rounded to 2 decimal places)")
        print("  Adding customer name and country")
        print("  Adding product category and sub-category")
        orders_enriched = enrich_order(orders_bronze, customers_enriched, products_enriched)
        print(f"  Enriched {orders_enriched.count():,} orders")
        
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
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
