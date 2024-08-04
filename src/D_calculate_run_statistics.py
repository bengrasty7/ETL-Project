from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

from src.utils import get_logger, get_spark_session
logger = get_logger("run_statistics", level="INFO")

# Function to calculate distance between two points
def calculate_distance(x1, y1, z1, x2, y2, z2):
    return F.sqrt((x2 - x1)**2 + (y2 - y1)**2 + (z2 - z1)**2)

def calculate_run_statistics(df):
    logger.info("Starting run statistics calculation")

    # Group by run_uuid and calculate statistics
    run_stats = df.groupBy("run_uuid").agg(
        F.min("time").alias("run_start_time"),
        F.max("time").alias("run_stop_time"),
        (F.unix_timestamp(F.max("time")) - F.unix_timestamp(F.min("time"))).alias("total_runtime_seconds")
    )

    # Calculate total distance traveled for each robot
    df = df.select("run_uuid", "time", "x_1", "y_1", "z_1", "x_2", "y_2", "z_2")
    for robot in [1, 2]:
        # Create a new dataframe with only valid coordinates for the robot
        robot_df = df.filter(
            (F.col(f"x_{robot}").isNotNull()) &
            (F.col(f"y_{robot}").isNotNull()) &
            (F.col(f"z_{robot}").isNotNull())
        ).select("run_uuid", "time", f"x_{robot}", f"y_{robot}", f"z_{robot}")

        # Create a window spec for the robot
        window_spec = Window.partitionBy("run_uuid").orderBy("time")

        # Calculate distance
        robot_df = robot_df.withColumn(
            f"dist_{robot}",
            calculate_distance(
                F.col(f"x_{robot}"),
                F.col(f"y_{robot}"),
                F.col(f"z_{robot}"),
                F.lag(f"x_{robot}").over(window_spec),
                F.lag(f"y_{robot}").over(window_spec),
                F.lag(f"z_{robot}").over(window_spec)
            )
        )

        # Calculate total distance for each run
        distance_per_run = robot_df.groupBy("run_uuid").agg(
            F.sum(f"dist_{robot}").alias(f"total_distance_robot_{robot}")
        )

        # Join the results back to run_stats
        run_stats = run_stats.join(distance_per_run, "run_uuid", "left_outer")

    return run_stats