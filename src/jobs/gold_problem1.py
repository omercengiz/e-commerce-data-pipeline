from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.spark import get_spark


def select_top_categories(category_events_df):
    """Select five categories per user from events before gender expansion.

    Category name breaks equal-count ties deterministically.
    """
    ranking = Window.partitionBy("userId").orderBy(
        F.col("event_count").desc(), F.col("categoryName").asc()
    )
    return (
        category_events_df.filter(F.col("categoryName").isNotNull())
        .groupBy("userId", "categoryName")
        .agg(F.count("*").alias("event_count"))
        .withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= 5)
        .select("userId", "categoryName")
    )


def calculate_click_averages(click_df):
    """Average the last three active days and the last three calendar days.

    Calendar days end at each user/category/gender's latest event. Missing
    calendar days contribute zero; active-day averages use available days.
    """
    keys = ["userId", "categoryName", "gender"]
    daily_counts = (
        click_df.withColumn("event_date", F.to_date("event_date"))
        .filter(F.col("event_date").isNotNull())
        .groupBy(*keys, "event_date")
        .agg(F.count("*").alias("daily_event_count"))
    )
    group_window = Window.partitionBy(*keys)
    recent_window = group_window.orderBy(F.col("event_date").desc())
    dated_counts = (
        daily_counts
        .withColumn("last_event_date", F.max("event_date").over(group_window))
        .withColumn("active_day_rank", F.row_number().over(recent_window))
    )
    return dated_counts.groupBy(*keys).agg(
        (F.sum(F.when(
            F.col("event_date") >= F.date_sub("last_event_date", 2),
            F.col("daily_event_count"),
        ).otherwise(0)) / F.lit(3.0)).alias("last3DayAverageClickCount"),
        F.avg(F.when(
            F.col("active_day_rank") <= 3, F.col("daily_event_count")
        )).alias("lastInteracted3DayAverageClickCount"),
    )


def build_problem1_output(click_metrics_df, order_metrics_df):
    """
    Joins clickstream metrics with order metrics.
    """

    return (
        click_metrics_df
        .join(
            order_metrics_df,
            ["userId", "categoryName", "gender"],
            "left"
        )
        .fillna(
            {
                "netPrice": 0.0,
                "grossItemCount": 0,
                "grossOrderCount": 0,
            }
        )
    )


def main():
    spark = get_spark("gold-problem-1")

    clickstream = spark.read.parquet("output/silver/clickstream")
    orders = spark.read.parquet("output/silver/order")

    # Rank original events, so multi-gender products do not receive extra votes.
    raw_events = spark.read.parquet("output/bronze/clickstream")
    product_categories = (
        spark.read.parquet("output/bronze/product")
        .select("productId", "categoryName").distinct()
    )
    category_events = raw_events.join(
        product_categories, raw_events.product_id == product_categories.productId,
        "inner",
    ).select(F.col("user_id").alias("userId"), "categoryName")
    top_categories = select_top_categories(category_events)
    selected_events = clickstream.join(
        top_categories, ["userId", "categoryName"], "left_semi"
    )
    click_metrics = calculate_click_averages(selected_events)
    result = build_problem1_output(click_metrics, orders)

    (
        result
        .write
        .mode("overwrite")
        .json("output/gold/problem1")
    )


if __name__ == "__main__":
    main()
