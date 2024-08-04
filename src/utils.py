from pyspark.sql import SparkSession
import os

def get_spark_session(app_name='ETL_Project'):
    """
    Create a Spark session with the specified number of threads.
    
    Parameters:
    app_name (str): The name of the Spark application.
    threads (int): The number of threads to use. Set to * to use all available cores.
    
    Returns:
    SparkSession: A Spark session object.
    """
    threads = os.cpu_count()
    master = f"local[{threads}]"
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_logger(name, level='INFO'):
    """
    Create a logger with the specified name and log level.
    
    Parameters:
    name (str): The name of the logger.
    level (str): The log level (e.g., 'INFO', 'DEBUG', 'ERROR').
    
    Returns:
    Logger: A logger object.
    """
    import logging
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger