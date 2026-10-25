from pyspark.sql import functions as F

from src.utils.spark import get_spark
from src.utils.schemas import CLICKSTREAM_SCHEMA, CLICKSTREAM_EVENTS


def read_clickstream(spark, input_path: str):
    """
    Read raw clickstream JSON data using an explicit schema.

    Using an explicit schema:
    - avoids schema inference cost
    - guarantees type safety
    """
    return (
        spark.read
        .schema(CLICKSTREAM_SCHEMA)
        .json(input_path)
    )


def transform_clickstream(df):
    """
    Apply basic Bronze-layer transformations:
    - Filter only valid clickstream events
    - Convert epoch timestamp (ms) to Spark timestamp
    - Derive event_date for downstream aggregations
    """
    return (
        df
        .filter(F.col("event").isin(CLICKSTREAM_EVENTS))
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("timestamp") / 1000)
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_ts"))
        )
    )



def main():
    """
    Bronze Clickstream Job

    Responsibilities:
    - Read raw clickstream data
    - Apply minimal transformations
    - Persist results in Parquet format for Silver layer
    """
    spark = get_spark("bronze-clickstream")

    input_path = "raw-data/clickstream"
    output_path = "output/bronze/clickstream"

    df_raw = read_clickstream(spark, input_path)
    df_bronze = transform_clickstream(df_raw)

    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(output_path)
    )


if __name__ == "__main__":
    main()
