from pyspark.sql import functions as F

from src.utils.spark import get_spark
from src.utils.schemas import PRODUCT_SCHEMA


def read_product(spark, input_path: str):
    """
    Read raw product JSON data using an explicit schema.

    Explicit schema usage:
    - prevents schema inference overhead
    - guarantees expected data types (especially ArrayType for gender)
    """
    return (
        spark.read
        .schema(PRODUCT_SCHEMA)
        .json(input_path)
    )


def transform_product(df):
    """
    Apply Bronze-layer product transformations:
    - Explode gender array into separate rows
    - Remove potential duplicates on (productId, gender)
    """
    return (
        df
        .withColumn("gender", F.explode_outer("gender"))
        .dropDuplicates(["productId", "gender"])
    )


def main():
    """
    Bronze Product Job

    Responsibilities:
    - Read raw product data
    - Normalize gender information
    - Persist results for downstream joins
    """
    spark = get_spark("bronze-product")

    input_path = "raw-data/product"
    output_path = "output/bronze/product"

    df_raw = read_product(spark, input_path)
    df_bronze = transform_product(df_raw)

    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(output_path)
    )


if __name__ == "__main__":
    main()
