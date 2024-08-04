import os
import sys
import logging
from operator import or_
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, format_string, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType

from src.utils import get_logger, get_spark_session
logger = get_logger("data_cleaning", level="INFO")
spark = get_spark_session(app_name="Data Cleaning")

# Check the schema
def validate_schema(df, expected_schema):
    actual_schema = df.schema
    if actual_schema != expected_schema:
        logger.error("Schema mismatch detected")
        logger.error(f"Expected schema: {expected_schema.simpleString()}")
        logger.error(f"Actual schema: {actual_schema.simpleString()}")
        raise ValueError("Schema does not match the expected schema")
    else:
        logger.info("Schema validation passed")
    
def convert_time(df, time_column='time'):
    logger.info(f"Converting {time_column} to timestamp")
    return df.withColumn(
        time_column,
        F.when(
            F.length(F.col(time_column)) == 20,  # Format: 2022-11-23T20:45:45Z
            F.concat(F.col(time_column).substr(1, 19), F.lit(".000Z"))
        ).otherwise(F.col(time_column))
    ).withColumn(
        time_column,
        F.to_timestamp(F.col(time_column), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ).withColumn(
        f"{time_column}_is_valid",
        F.to_timestamp(F.col(f"{time_column}"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").isNotNull()
    )

def check_nulls(df, col_name, threshold=0.1):
    """
    Check if a column has null values and throw an error if more than 10% of values are null
    """
    num_nulls = df.filter(F.col(col_name).isNull()).count()
    if num_nulls > 0:
        logger.warning(f"Found {num_nulls} null values for column {col_name}")
        if num_nulls / df.count() > threshold:
            logger.error(f"More than 10% of values for column {col_name} are null, exiting")
            spark.stop()
            raise ValueError("More than 10% of values are null for column {col_name}")
    else:
        logger.info(f"No null values in column {col_name}")

def fill_defaults(df, col_name):
    """
    Fill default values for columns.
    If you want to have more default columns/values, add them to the defaults dictionary.
    """
    # count null values and log
    num_nulls = df.filter(F.col(col_name).isNull()).count()
    if num_nulls == 0:
        logger.info(f"No null values in column {col_name}")
        return df
    
    logger.warning(f"Found {num_nulls} null values in column {col_name}")

    defaults = {
        "time": "2022-11-23T00:00:00Z",
    }

    if col_name in defaults:
        df = df.fillna(defaults[col_name], subset=[col_name])
    elif col_name == "sensor_type":
        # fill with 'encoder' if field is x,y,z and sensor_type if field is fx,fy,fz
        df = df.withColumn(col_name,
                           F.when(F.col("field").isin(["x", "y", "z"]), "encoder")
                            .when(F.col("field").isin(["fx", "fy", "fz"]), "load_cell")
                            .otherwise(None))
        
    logger.info(f"Filled null values in column {col_name} with default value")        
    return df
        
def check_acceptable_values(df, col_name, values, threshold=0.1):
    """
    Check if values in a column are in an acceptable list of values, otherwise fill with None
    """
    invalid_count = df.filter(~F.col(col_name).isin(values)).count()
    if invalid_count > 0:
        logger.warning(f"Column {col_name} has {invalid_count} invalid values.")
        # Fill invalid values with None
        df = df.withColumn(col_name, F.when(~F.col(col_name).isin(values), None).otherwise(F.col(col_name)))
        # Check if more than 10% of values are invalid
        if invalid_count / df.count() > threshold:
            logger.error(f"More than 10% of values in column {col_name} are invalid, exiting")
            spark.stop()
            raise ValueError(f"More than 10% of values in column {col_name} are invalid")
    else:
        logger.info(f"All values in column {col_name} are valid")

def check_value_ranges(df, value_ranges):
    """
    Check if values in the value column are within acceptable ranges
    """
    conditions = []
    for column, ranges in value_ranges.items():
        for field, (min_val, max_val) in ranges.items():
            condition = (
                (F.col('field') == field) & 
                (F.col(column).between(min_val, max_val))
            )
            conditions.append(condition)

    # Combine all conditions
    all_conditions = reduce(or_, conditions)

    # Apply the conditions and create a new column for validity
    df = df.withColumn('is_valid_range', 
                       F.when(all_conditions, True)
                       .otherwise(False))
    
    # Count invalid entries
    invalid_count = df.filter(~F.col('is_valid_range')).count()

    if invalid_count > 0:
        # Throw error if number of outlier entries if greater than 10%, otherwise drop rows
        if invalid_count / df.count() > 0.1:
            logger.error("More than 10% of values are outside valid ranges, exiting")
            spark.stop()
            raise ValueError("More than 10% of values are outside valid ranges")
        else:
            logger.warning(f"Found {invalid_count} values outside valid ranges, dropping rows")
            df = df.filter(F.col('is_valid_range'))
    else:
        logger.info("All values are within valid ranges")
    return df

def validate_and_clean_data(df):
    # Define the expected schema
    expected_schema = StructType([
        StructField('time', StringType(), True),
        StructField('value', DoubleType(), True),
        StructField('field', StringType(), True),
        StructField('robot_id', IntegerType(), True),
        StructField('run_uuid', StringType(), False),
        StructField('sensor_type', StringType(), True)
    ])

    # Convert run_uuid to a string without decimal places
    df = df.withColumn("run_uuid", format_string("%.0f", col("run_uuid")))

    # Cast robot_id to integer
    df = df.withColumn("robot_id", df["robot_id"].cast(IntegerType()))

    # Explicitly cast columns to desired types
    df = df.withColumn("time", col("time").cast(StringType())) \
        .withColumn("value", col("value").cast(DoubleType())) \
        .withColumn("field", col("field").cast(StringType())) \
        .withColumn("sensor_type", col("sensor_type").cast(StringType()))

    # Validate the schema
    try:
        validate_schema(df, expected_schema)
    except ValueError as e:
        logger.error(f"Schema validation failed: {e}")
        spark.stop()
        raise

    # clean time values and convert to timestamp
    df = convert_time(df)

    # Fill default values for inconsequential columns
    default_columns = ["time", "sensor_type"]
    for col_name in default_columns:
        fill_defaults(df, col_name)

    # Check for null values in columns, if below threshold, we can drop rows
    null_checks = ["value", "field", "robot_id", "run_uuid"]
    for col_name in null_checks:
        check_nulls(df, col_name)
    df = df.dropna()

    # Check for acceptable values in columns, if below threshold, we can drop rows
    acceptable_values = {
        'robot_id': [1, 2],
        'field': ['x', 'y', 'z', 'fx', 'fy', 'fz'],
        'sensor_type': ['encoder', 'load_cell']
    }
    for col_name, values in acceptable_values.items():
        check_acceptable_values(df, col_name, values)
    df = df.dropna()
    
    # Check for outliers in the value column (roughly 3 standard deviations from the mean)
    value_ranges = {
        'value': {
            'x': (500, 3500),
            'y': (250, 1500),
            'z': (-1000, 0),
            'fx': (-1750, 750),
            'fy': (-1000, 1000),
            'fz': (-2000, 100)
        }
    }
    df = check_value_ranges(df, value_ranges)

    logger.info("Data validation complete")
    return df
