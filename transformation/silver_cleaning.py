from pyspark.sql import SparkSession
import os
import sys


def process_silver_layer():
    print("Starting Pyspark SparkSession...")
    spark= SparkSession.builder \
        .appName("TechJobMarket_SilverLayer") \
        .getOrCreate()
    current_dir= os.path.dirname(os.path.abspath(__file__))
    project_root= os.path.dirname(current_dir)
    raw_path=os.path.join(project_root,"storage", "bronze")
    linux_style_path = raw_path.replace("\\", "/") 
    
    bronze_path = f"{linux_style_path}/*.json"
    print(f"Reading raw data from: {bronze_path}")
    raw_df= spark.read.json(bronze_path)
    initial_count= raw_df.count()
    print(f"Total rows before cleaning: {initial_count}")

    # Create a virtual SQL table from our DataFrame
    raw_df.createOrReplaceTempView("raw_jobs")
    # Deduplicate and Flatten
    sql_query= """
        SELECT
            id,
            FIRST(title) AS job_title,
            FIRST(location) AS location_name,
            FIRST(published) AS created_date,
            FIRST(description) AS job_description
        FROM raw_jobs
        GROUP BY id
    """
    clean_df= spark.sql(sql_query)
    final_count= clean_df.count()
    print(f"Total rows after SQL deduplication: {final_count}")
    print(f"We removed {initial_count - final_count} duplicate rows!")
    print("\nSample of clean data: ")
    clean_df.show(5,truncate=False)
    spark.stop()
if __name__== "__main__":
    process_silver_layer()