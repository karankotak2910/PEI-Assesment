"""
SQL Analytics - Business Intelligence Queries
Query Gold schema aggregate table from Unity Catalog
"""

import sys
sys.path.insert(0, '.')

from src.data_loader import get_spark_session, load_gold_table


def main():
    spark = get_spark_session("SQL_Analytics")
    
    try:
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
        print("Query 3: Profit by Customer (Top 20)")
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
        LIMIT 20
        """
        spark.sql(query3).show(20, truncate=False)
        
        print("\n" + "="*80)
        print("Query 4: Profit by Customer + Year (Top 20)")
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
        LIMIT 20
        """
        spark.sql(query4).show(20, truncate=False)
        
        print("\n" + "="*80)
        print("Additional Analysis")
        print("="*80)
        
        print("\nProfit by Year + Category + Sub-Category (Top 15):")
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
        LIMIT 15
        """
        spark.sql(query5).show(15, truncate=False)
        
        print("\nTop 5 Categories Overall:")
        query6 = """
        SELECT 
            category,
            SUM(total_profit) as category_profit,
            SUM(order_count) as total_orders
        FROM profit_aggregate
        GROUP BY category
        ORDER BY category_profit DESC
        LIMIT 5
        """
        spark.sql(query6).show(truncate=False)
        
        print("\nSQL Analytics Complete")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
