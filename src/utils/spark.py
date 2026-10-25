from pyspark.sql import SparkSession


def get_spark(app_name: str = "E-Ticaret-case") -> SparkSession:
    """
    Create and return a SparkSession configured for local development.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark
