from pyspark.sql import functions as F
from src.utils.spark import get_spark


def calculate_net_gross(order_df):
    """
    Calculate net and gross order metrics per user and category.

    Definitions:
    - grossItemCount  : Number of distinct itemIds (all items, regardless of status)
    - grossOrderCount : Number of distinct orderIds
    - netPrice        : Sum of latest prices for non-cancelled/non-returned items
                         (CANCELLED and RETURN_ACCEPTED are excluded)

    Input:
    - Enriched order history containing product metadata and Bronze is_last_status

    Output:
    - Aggregated DataFrame grouped by (userId, categoryName, gender)
    """
    return (
        order_df.filter(F.col("is_last_status"))
        .groupBy("userId", "categoryName", "gender")
        .agg(
            F.countDistinct("itemId").alias("grossItemCount"),
            F.countDistinct("orderId").alias("grossOrderCount"),
            F.sum(
                F.when(~F.col("status").isin("CANCELLED", "RETURN_ACCEPTED"), F.col("price"))
                 .otherwise(0.0)
            ).alias("netPrice")
        )
    )


def main():
    """
    Silver Order Job

    Responsibilities:
    - Read Bronze order data
    - Enrich orders with product metadata (category & gender)
    - Calculate net and gross order metrics

    Notes:
    - Product gender is already exploded at the Bronze Product layer
    - Aggregations are intentionally kept simple and explicit
    - Output is prepared for Gold layer analytical problems
    """
    spark = get_spark("silver-order")

    orders = spark.read.parquet("output/bronze/order")
    products = spark.read.parquet("output/bronze/product")

    enriched_orders = (
        orders
        .join(products, on="productId", how="left")
    )

    result = calculate_net_gross(enriched_orders)

    (
        result
        .write
        .mode("overwrite")
        .parquet("output/silver/order")
    )


if __name__ == "__main__":
    main()
