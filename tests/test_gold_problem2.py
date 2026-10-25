from datetime import datetime, timezone

from src.jobs.gold_problem2 import calculate_clusters, main


def test_cluster_changes_are_chronological_and_singletons_are_excluded(spark):
    data = [(1, day) for day, count in [
        ("2023-10-01", 2), ("2023-10-02", 8), ("2023-10-03", 4),
        ("2023-10-06", 9), ("2023-10-07", 3), ("2023-10-10", 20),
    ] for _ in range(count)]
    data += [(2, "2023-10-01"), (2, "2023-10-03")]
    df = spark.createDataFrame(data, "userId long, event_date string").repartition(3)
    rows = sorted(calculate_clusters(df).collect(), key=lambda r: r.firstDayOfSet)
    assert len(rows) == 2
    assert all(r.userId == 1 and r.maxInteractedDayCount == 3 for r in rows)
    assert rows[0].firstDayOfSet == int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert rows[0].avgClickCount == 4.67
    assert rows[0].avgIncrease == 1.0
    assert rows[1].avgClickCount == 6.0
    assert rows[1].avgIncrease == -6.0


def test_problem2_reads_original_events_without_product_expansion(spark, tmp_path, monkeypatch, pipeline_directory):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.jobs.gold_problem2.get_spark", lambda _: spark)
    raw = spark.createDataFrame([(1, "2023-10-01"), (1, "2023-10-02")],
                                "user_id long, event_date string")
    raw.write.parquet("output/bronze/clickstream")
    # No Silver/product data is available: the job must work from original events.
    main()
    row = spark.read.json("output/gold/problem2").first()
    assert row.avgClickCount == 1.0
    assert row.avgIncrease == 0.0


def test_empty_events(spark):
    df = spark.createDataFrame([], "userId long, event_date date")
    assert calculate_clusters(df).count() == 0
