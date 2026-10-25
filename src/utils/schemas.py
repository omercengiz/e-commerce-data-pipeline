from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    ArrayType,
    DoubleType,
)


CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), nullable=False),
        StructField("platform", StringType(), nullable=True),
        StructField("product_id", LongType(), nullable=True),
        StructField("event", StringType(), nullable=True),
        StructField("sid", StringType(), nullable=True),
        StructField("timestamp", LongType(), nullable=False),
    ]
)


PRODUCT_SCHEMA = StructType(
    [
        StructField("productId", LongType(), nullable=False),
        StructField("categoryName", StringType(), nullable=True),
        StructField("gender", ArrayType(StringType()), nullable=True),
    ]
)

ORDER_SCHEMA = StructType(
    [
        StructField("orderId", LongType(), nullable=False),
        StructField("userId", LongType(), nullable=False),
        StructField("orderDate", LongType(), nullable=False),
        StructField("price", DoubleType(), nullable=True),
        StructField("itemId", LongType(), nullable=False),
        StructField("productId", LongType(), nullable=False),
        StructField("statusCreatedDate", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
    ]
)


CLICKSTREAM_EVENTS = {
    "impression",
    "add_to_fav",
    "add_to_basket",
    "purchase",
}

ORDER_STATUSES = {
    "CREATED",
    "DELIVERED",
    "CANCELLED",
    "RETURN_ACCEPTED",
}

