import pytest
from pyspark.sql import Row

from src.jobs.gold_problem1 import (
    build_problem1_output, calculate_click_averages, select_top_categories,
)


def events(user, category, days, gender="female"):
    return [Row(userId=user, categoryName=category, gender=gender, event_date=day)
            for day, count in days for _ in range(count)]


def test_active_days_differ_from_calendar_days_and_keep_old_users(spark):
    data = events(1, "Cardigan", [("2023-10-01", 8), ("2023-10-05", 2),
                              ("2023-10-10", 4), ("2023-10-11", 6)])
    data += events(2, "Ballet Flats", [("2023-11-01", 3)])
    rows = {r.userId: r for r in calculate_click_averages(spark.createDataFrame(data)).collect()}
    assert set(rows) == {1, 2}
    assert rows[1].last3DayAverageClickCount == pytest.approx(10 / 3)
    assert rows[1].lastInteracted3DayAverageClickCount == 4.0
    assert rows[2].last3DayAverageClickCount == 1.0
    assert rows[2].lastInteracted3DayAverageClickCount == 3.0


def test_top_categories_are_five_per_user_with_deterministic_ties(spark):
    data = [(1, category) for category in "ABCDEF"] + [(1, "F"), (2, "Z")]
    df = spark.createDataFrame(data, "userId long, categoryName string")
    rows = select_top_categories(df.repartition(2)).collect()
    assert {r.categoryName for r in rows if r.userId == 1} == set("ABCDF")
    assert {r.categoryName for r in rows if r.userId == 2} == {"Z"}


def test_orderless_categories_get_zero_order_metrics(spark):
    clicks = calculate_click_averages(spark.createDataFrame(events(1, "Cardigan", [("2023-10-20", 3)])))
    orders = spark.createDataFrame([], "userId long, categoryName string, gender string, netPrice double, grossItemCount long, grossOrderCount long")
    row = build_problem1_output(clicks, orders).first()
    assert (row.netPrice, row.grossItemCount, row.grossOrderCount) == (0.0, 0, 0)


def test_empty_clicks(spark):
    df = spark.createDataFrame([], "userId long, categoryName string, gender string, event_date date")
    assert calculate_click_averages(df).count() == 0


def test_problem1_ranks_original_events_before_gender_expansion(spark, tmp_path, monkeypatch, pipeline_directory):
    from src.jobs.gold_problem1 import main
    from src.jobs.silver_clickstream import enrich_clickstream

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.jobs.gold_problem1.get_spark", lambda _: spark)
    raw = spark.createDataFrame(
        [(1, i, "impression", "2023-10-01")
         for i in range(6) for _ in range(4 if i == 5 else 3)],
        "user_id long, product_id long, event string, event_date string",
    )
    # E has two genders but must lose the five-way count tie to A/B/C/D.
    product_rows = [(i, category, "female") for i, category in enumerate("ABCDEF")]
    product_rows.append((4, "E", "male"))
    products = spark.createDataFrame(product_rows,
                                     "productId long, categoryName string, gender string")
    raw.write.parquet("output/bronze/clickstream")
    products.write.parquet("output/bronze/product")
    enrich_clickstream(raw, products).write.parquet("output/silver/clickstream")
    spark.createDataFrame([], "userId long, categoryName string, gender string, netPrice double, grossItemCount long, grossOrderCount long").write.parquet("output/silver/order")
    main()
    rows = spark.read.json("output/gold/problem1").collect()
    assert {r.categoryName for r in rows} == set("ABCDF")
    assert len(rows) == 5
