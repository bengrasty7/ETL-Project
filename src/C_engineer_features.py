import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
from pyspark.sql.types import DoubleType
import pandas as pd

from src.utils import get_logger
logger = get_logger("engineer_features", level="INFO")

def calculate_engineered_features(df):
    logger.info("Starting engineered feature calculation")

    # Define window for lag calculations
    window_spec = Window.partitionBy("run_uuid").orderBy("time")

    logger.info("Calculating time differences")

    # Use the lag function to get the previous timestamp
    df = df.withColumn("prev_time", F.lag("time").over(window_spec))

    # Calculate time difference
    @F.pandas_udf(DoubleType())
    def time_diff_udf(current_time: pd.Series, prev_time: pd.Series) -> pd.Series:
        diff = (current_time - prev_time).dt.total_seconds()
        return diff
    
    df = df.withColumn("time_diff", time_diff_udf(F.col("time"), F.col("prev_time")))
    
    logger.info("Calculating velocities and accelerations")
    # Calculate velocities and accelerations
    for axis in ['x', 'y', 'z']:
        for robot in [1, 2]:
            logger.info(f"Processing {axis}-axis for robot {robot}")
            # Velocity
            df = df.withColumn(
                f"v{axis}_{robot}",
                F.when(F.col("time_diff") != 0,
                    (F.col(f"{axis}_{robot}") - F.lag(f"{axis}_{robot}").over(window_spec)) / F.col("time_diff")
                ).otherwise(0)
            )

            # Acceleration
            df = df.withColumn(
                f"a{axis}_{robot}",
                F.when(F.col("time_diff") != 0,
                    (F.col(f"v{axis}_{robot}") - F.lag(f"v{axis}_{robot}").over(window_spec)) / F.col("time_diff")
                ).otherwise(0)
            )

    logger.info("Calculating total velocities, accelerations, and forces")
    # Calculate total velocities, accelerations, and forces
    for robot in [1, 2]:
        df = df.withColumn(
            f"v{robot}",
            F.sqrt(F.col(f"vx_{robot}")**2 + F.col(f"vy_{robot}")**2 + F.col(f"vz_{robot}")**2)
        ).withColumn(
            f"a{robot}",
            F.sqrt(F.col(f"ax_{robot}")**2 + F.col(f"ay_{robot}")**2 + F.col(f"az_{robot}")**2)
        ).withColumn(
            f"f{robot}",
            F.sqrt(F.col(f"fx_{robot}")**2 + F.col(f"fy_{robot}")**2 + F.col(f"fz_{robot}")**2)
        )

    # Log new columns
    new_columns = set(df.columns) - set(['time', 'x_1', 'y_1', 'z_1', 'x_2', 'y_2', 'z_2', 
                                         'fx_1', 'fy_1', 'fz_1', 'fx_2', 'fy_2', 'fz_2'])
    logger.info(f"New columns added: {', '.join(new_columns)}")

    return df

