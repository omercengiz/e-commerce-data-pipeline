from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.spark import get_spark


def calculate_clusters(click_df):
    """Compute metrics for runs of at least two consecutive active days.

    Input must contain original events, before any product/gender expansion.
    avgIncrease is the mean daily count difference within each run.
    """
    daily_counts = (
        click_df.withColumn("event_date", F.to_date("event_date"))
        .filter(F.col("event_date").isNotNull())
        .groupBy("userId", "event_date")
        .agg(F.count("*").alias("daily_event_count"))
    )
    chronological = Window.partitionBy("userId").orderBy("event_date")
    clustered = (
        daily_counts.withColumn("rn", F.row_number().over(chronological))
        .withColumn("cluster_key", F.expr("date_sub(event_date, rn)"))
    )
    within_cluster = Window.partitionBy("userId", "cluster_key").orderBy("event_date")
    changes = clustered.withColumn(
        "daily_change",
        F.col("daily_event_count") - F.lag("daily_event_count").over(within_cluster),
    )
    clusters = (
        changes.groupBy("userId", "cluster_key")
        .agg(
            F.min("event_date").alias("first_day"),
            F.avg("daily_event_count").alias("avgClickCount"),
            F.count("*").alias("dayCount"),
            F.avg("daily_change").alias("avgIncrease"),
        )
        .filter(F.col("dayCount") >= 2)
    )
    return (
        clusters.withColumn(
            "maxInteractedDayCount",
            F.max("dayCount").over(Window.partitionBy("userId")),
        )
        .select(
            "userId",
            (F.col("first_day").cast("timestamp").cast("long") * 1000)
            .alias("firstDayOfSet"),
            F.round("avgClickCount", 2).alias("avgClickCount"),
            "maxInteractedDayCount",
            F.round("avgIncrease", 2).alias("avgIncrease"),
        )
    )


def main():
    spark = get_spark("gold-problem-2")

    clickstream = spark.read.parquet("output/bronze/clickstream").withColumnRenamed("user_id", "userId")

    result = calculate_clusters(clickstream)

    (
        result
        .write
        .mode("overwrite")
        .json("output/gold/problem2")
    )


if __name__ == "__main__":
    main()
