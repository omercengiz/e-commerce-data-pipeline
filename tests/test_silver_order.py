import pytest
from src.jobs.bronze_order import transform_order
from src.jobs.silver_order import calculate_net_gross
from src.utils.schemas import ORDER_SCHEMA


def test_latest_status_excludes_returns_and_counts_created_items_once(spark):
    data = []
    histories = [
        (1, 100, 100.0, ["CREATED", "DELIVERED", "RETURN_ACCEPTED"]),
        (2, 100, 50.0, ["CREATED", "DELIVERED"]),
        (3, 101, 25.0, ["CREATED"]),
        (4, 102, 75.0, ["CREATED", "CANCELLED"]),
    ]
    for item, order, price, statuses in histories:
        for i, status in enumerate(statuses):
            data.append((order, 1, 1697749549625, price, item, 10, 1697749549625 + i * 1000, status))
    # A repeated delivered record must not add the price again.
    data.append(data[4])
    bronze = transform_order(spark.createDataFrame(data, ORDER_SCHEMA))
    products = spark.createDataFrame([(10, "Cardigan", "female"), (10, "Cardigan", "male")],
                                     "productId long, categoryName string, gender string")
    rows = calculate_net_gross(bronze.join(products, "productId")).collect()
    assert len(rows) == 2
    for row in rows:
        assert row.netPrice == pytest.approx(75.0)
        assert row.grossItemCount == 4
        assert row.grossOrderCount == 3


def test_terminal_status_wins_equal_timestamp(spark):
    data = [(100, 1, 1000, 10.0, 1, 10, 2000, status)
            for status in ["CREATED", "DELIVERED", "RETURN_ACCEPTED"]]
    rows = transform_order(spark.createDataFrame(data, ORDER_SCHEMA).repartition(2))
    assert rows.filter("is_last_status").first().status == "RETURN_ACCEPTED"
