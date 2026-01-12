"""
Gold Layer - Business Aggregates
Load from Silver schema, aggregate, and save to Gold schema in Unity Catalog
"""

import sys
sys.path.insert(0, '.')

from src.data_loader import get_spark_session, load_silver_table, save_gold_table
from src.transformations import create_profit_aggregate


def main():
    spark = get_spark_session("Gold_Aggregation")
    
    try:
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
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
