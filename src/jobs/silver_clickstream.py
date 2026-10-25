from pyspark.sql import functions as F

from src.utils.spark import get_spark


def enrich_clickstream(clickstream_df, product_df):
    """
    Enrich clickstream events with product metadata.
    Gender is already exploded at the Bronze Product layer.
    """

    return (
        clickstream_df
        .join(
            product_df,
            clickstream_df.product_id == product_df.productId,
            how="left"
        )
        .select(
            F.col("user_id").alias("userId"),
            "categoryName",
            "gender",        # already STRING
            "event",
            "event_date"
        )
    )


def main():
    """
    Silver Clickstream Job

    Responsibilities:
    - Read Bronze clickstream data
    - Join with Bronze product data
    - Produce an enriched, analytics-ready clickstream dataset

    This layer:
    - Does NOT perform aggregations
    - Does NOT apply business KPIs
    - Prepares clean input for Gold layer problems
    """
    spark = get_spark("silver-clickstream")

    clickstream = spark.read.parquet("output/bronze/clickstream")
    product = spark.read.parquet("output/bronze/product")

    enriched_df = enrich_clickstream(clickstream, product)

    (
        enriched_df
        .write
        .mode("overwrite")
        .parquet("output/silver/clickstream")
    )


if __name__ == "__main__":
    main()
