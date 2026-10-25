import importlib
import json


def test_json_to_gold_pipeline(spark, tmp_path, monkeypatch, pipeline_directory):
    """Run all seven jobs with raw JSON and verify both final contracts."""
    monkeypatch.chdir(tmp_path)
    raw = {
        "product": [{"productId": 10, "categoryName": "Hırka", "gender": ["female", "male"]}],
        "clickstream": [
            {"user_id": 1, "product_id": 10, "event": "impression", "platform": "web",
             "sid": "session", "timestamp": timestamp}
            for timestamp, count in [(1696118400000, 2), (1696204800000, 4), (1696896000000, 5)]
            for _ in range(count)
        ],
        "order": [
            {"orderId": 100, "userId": 1, "orderDate": 1696118400000,
             "price": 100.0, "itemId": 1, "productId": 10,
             "statusCreatedDate": 1696118400000 + i * 1000, "status": status}
            for i, status in enumerate(["CREATED", "DELIVERED", "RETURN_ACCEPTED"])
        ],
    }
    for name, records in raw.items():
        directory = tmp_path / "raw-data" / name
        directory.mkdir(parents=True)
        (directory / f"{name}.json").write_text(
            "\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8"
        )
    for name in ["bronze_clickstream", "bronze_product", "bronze_order", "silver_clickstream",
                 "silver_order", "gold_problem1", "gold_problem2"]:
        job = importlib.import_module(f"src.jobs.{name}")
        monkeypatch.setattr(job, "get_spark", lambda _: spark)
        job.main()

    problem1 = spark.read.json("output/gold/problem1")
    assert set(problem1.columns) == {
        "userId", "categoryName", "gender", "last3DayAverageClickCount",
        "lastInteracted3DayAverageClickCount", "netPrice", "grossItemCount", "grossOrderCount",
    }
    rows = problem1.collect()
    assert {r.gender for r in rows} == {"female", "male"}
    assert len(rows) == 2
    for row in rows:
        assert row.last3DayAverageClickCount == 5 / 3
        assert row.lastInteracted3DayAverageClickCount == 11 / 3
        assert (row.netPrice, row.grossItemCount, row.grossOrderCount) == (0.0, 1, 1)
    problem2 = spark.read.json("output/gold/problem2")
    assert set(problem2.columns) == {
        "userId", "firstDayOfSet", "avgClickCount", "maxInteractedDayCount", "avgIncrease",
    }
    rows = problem2.collect()
    assert len(rows) == 1
    assert rows[0].firstDayOfSet == 1696118400000
    assert rows[0].avgClickCount == 3.0
    assert rows[0].maxInteractedDayCount == 2
    assert rows[0].avgIncrease == 2.0
