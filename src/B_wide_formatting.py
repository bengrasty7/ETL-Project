import sys
from pyspark.sql import functions as F
from utils import get_logger, get_spark_session
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
logger = get_logger("wide_formatting", level="INFO")

def time_alignment(df, round_to=10):
    """
    Align the time column to the nearest bin_duration.
    
    Parameters:
    df (DataFrame): A PySpark DataFrame
    round_to (int): The bin duration to which the time column should be aligned. Default is 10 (every 10 milleseconds).
    Returns:
    DataFrame: A PySpark DataFrame with the time column aligned to the nearest bin_duration.
    """
    @F.udf(TimestampType())
    def round_to_nearest(timestamp, run_uuid):
        if run_uuid == '6176976534744076300':
            round_to = 10000 # 10 milliseconds
        elif run_uuid == '7582293080991469600':
            round_to = 10000
        elif run_uuid == '8910095844186656800':
            round_to = 10000
        elif run_uuid == '12405186538561671000':
            round_to = 10000
        else:
            logger.error(f"Unknown run_uuid: {run_uuid}")
            raise ValueError(f"Unknown run_uuid: {run_uuid}")
        microseconds = timestamp.microsecond
        rounded_microseconds = (microseconds // round_to) * round_to
        return timestamp.replace(microsecond=rounded_microseconds)

    # Round the time column
    df = df.withColumn("rounded_time", 
                               round_to_nearest(F.col('time'), F.col('run_uuid')))
    
    # filter for run_uuid 6176976534744076300, select time and rounded time, sort times and show
    # z values are null for robot 2, so they will not make it into the wide format
    # df.filter(F.col('run_uuid') == '6176976534744076300').select('time', 'rounded_time', 'x_1', 'y_1', 'z_1', 'fx_1', 'fy_1', 'fz_1', 'x_2', 'y_2', 'z_2', 'fx_2', 'fy_2', 'fz_2').sort('time').show(truncate=False)

    # filter for run_uuid 7582293080991469600, select time and rounded time, sort times and show
    # df.filter(F.col('run_uuid') == '7582293080991469600').select('time', 'rounded_time', 'x_1', 'y_1', 'z_1', 'fx_1', 'fy_1', 'fz_1', 'x_2', 'y_2', 'z_2', 'fx_2', 'fy_2', 'fz_2').sort('time').show(truncate=False)

    columns_to_fill = ['x_1', 'y_1', 'z_1', 'x_2', 'y_2', 'z_2', 
                       'fx_1', 'fy_1', 'fz_1', 'fx_2', 'fy_2', 'fz_2']
    
    # Aggregate values for each rounded time bin
    logger.info("Aggregating values for each rounded time bin")
    agg_expressions = [F.first(col, ignorenulls=True).alias(col) for col in columns_to_fill]
    df = df.groupBy("rounded_time", "run_uuid").agg(*agg_expressions)

    df = df.dropna(subset=columns_to_fill)
    
    # tested interpolation, too slow
    # window_time = Window.orderBy('time_bin')
    # logger.info("Filling null values with the average of the previous and next values")
    # # Perform filling. If the value is null, fill it with the average of the previous and next values
    # for col in columns_to_fill:
    #     df = df.withColumn(
    #         f"{col}_interpolated",
    #         F.when(F.col(col).isNotNull(), 
    #                F.col(col))
    #         .otherwise(
    #             (F.last(col, True).over(window_time) 
    #              + F.first(col, True).over(window_time.rowsBetween(1, Window.unboundedFollowing))) 
    #              / 2
    #         )
    #     )

    # rename rounded_time to time
    df = df.withColumnRenamed("rounded_time", "time")
    
    return df

def convert_to_wide_format(df):
    """
    Convert the input DataFrame from long to wide format.
    
    Parameters:
    df (DataFrame): A PySpark DataFrame in long format.
    
    Returns:
    DataFrame: A PySpark DataFrame in wide format.
    """
    logger.info("Converting data to wide format")

    df = df.withColumn("field_robot", F.concat(F.col("field"), F.lit("_"), F.col("robot_id")))

    # Pivot the data
    wide_df = df.groupBy("time", "run_uuid").pivot("field_robot").agg(F.first("value"))

    # Align the time column
    wide_full_df = time_alignment(wide_df)
        
    return wide_df, wide_full_df