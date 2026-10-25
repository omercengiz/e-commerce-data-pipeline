from pyspark.sql import Row


def test_enrich_clickstream_joins_product_with_gender(spark):
    clickstream = spark.createDataFrame([
        Row(user_id=1, product_id=10, event="impression", event_date="2023-10-20")
    ])

    product = spark.createDataFrame([
        Row(productId=10, categoryName="Hırka", gender="female"),
        Row(productId=10, categoryName="Hırka", gender="male"),
    ])

    from src.jobs.silver_clickstream import enrich_clickstream

    result = enrich_clickstream(clickstream, product)
    rows = result.collect()

    assert len(rows) == 2
    assert set(r.gender for r in rows) == {"female", "male"}
    assert all(r.categoryName == "Hırka" for r in rows)
    assert all(r.userId == 1 for r in rows)
