import os
import logging
from pyspark.sql import SparkSession
import sys
import os
import duckdb
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.A_data_cleaning import validate_and_clean_data
from src.B_wide_formatting import convert_to_wide_format
from src.C_engineer_features import calculate_engineered_features
from src.D_calculate_run_statistics import calculate_run_statistics
from src.utils import get_spark_session, get_logger

# Configure Spark session
spark = get_spark_session(app_name='ETL_Project')
logger = get_logger("etl_pipeline", level="INFO")


def save_to_duckdb(df, db_path, table_name):
    pandas_df = df.toPandas()

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    # Drop table if it already exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Create table and insert data
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM pandas_df")

    # Close the connection
    con.close()
    
    logger.info(f"Data saved to DuckDB table: {table_name}")

def main():
    raw_data_path = 'data/sample.parquet'
    db_path = 'data/robot_data.db'

    # Load the data
    df = spark.read.parquet(raw_data_path)

    # Repartition the data to parallelize the processing
    threads = os.cpu_count()
    df = df.repartition(threads)

    # Step 1: Preprocess and Clean
    cleaned_data_df = validate_and_clean_data(df)
    
    # Step 2: Convert to Wide Format
    wide_data_df, wide_data_full_df = convert_to_wide_format(cleaned_data_df)

    # # Step 3: Feature Engineering
    features_df = calculate_engineered_features(wide_data_full_df)
    
    # # Step 4: Calculate Runtime Statistics
    statistics = calculate_run_statistics(wide_data_df)

    # # Step 5: Save data to DuckDB
    save_to_duckdb(features_df, db_path, "features")
    save_to_duckdb(statistics, db_path, "statistics")

    logger.info(f"ETL pipeline completed, saved data to DuckDB at {db_path}")

if __name__ == '__main__':
    main()