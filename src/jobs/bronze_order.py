from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark import get_spark
from src.utils.schemas import ORDER_SCHEMA, ORDER_STATUSES


def read_order(spark, input_path: str):
    """
    Read raw order data with an explicit schema.
    """
    return (
        spark.read
        .schema(ORDER_SCHEMA)
        .json(input_path)
    )


def transform_order(df):
    """
    Bronze-level transformations for order data.

    - Filter only valid order statuses
    - Convert epoch millisecond timestamps to Spark timestamps
    - Derive the latest status per itemId
    """

    # 1. Filter valid order statuses (data quality enforcement)
    df = df.filter(F.col("status").isin(ORDER_STATUSES))

    # 2. Convert timestamps (epoch millis -> timestamp)
    df = (
        df
        .withColumn("order_ts", F.to_timestamp(F.col("orderDate") / 1000))
        .withColumn("status_ts", F.to_timestamp(F.col("statusCreatedDate") / 1000))
    )

    # 3. Window to determine last status per itemId
    window_item = (
        Window
        .partitionBy("itemId")
        .orderBy(
            F.col("status_ts").desc(),
            F.when(F.col("status") == "RETURN_ACCEPTED", 4)
             .when(F.col("status") == "CANCELLED", 3)
             .when(F.col("status") == "DELIVERED", 2)
             .otherwise(1).desc(),
            F.col("orderId").asc(),
            F.col("productId").asc(),
            F.col("price").desc(),
        )
    )

    df = (
        df
        .withColumn("rn", F.row_number().over(window_item))
        .withColumn("is_last_status", F.col("rn") == 1)
        .drop("rn")  # cleanup helper column
    )

    return df


def main():
    spark = get_spark("bronze-order")

    input_path = "raw-data/order"
    output_path = "output/bronze/order"

    df_raw = read_order(spark, input_path)
    df_bronze = transform_order(df_raw)

    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(output_path)
    )


if __name__ == "__main__":
    main()
