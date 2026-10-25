from datetime import date

from src.jobs.bronze_clickstream import transform_clickstream
from src.utils.schemas import CLICKSTREAM_SCHEMA


def test_epoch_milliseconds_use_utc_day_and_invalid_events_are_filtered(spark):
    data = [
        (1, "web", 10, "impression", "session", 1698796799999),
        (1, "web", 10, "purchase", "session", 1698796800000),
        (1, "web", 10, "invalid", "session", 1698796800000),
    ]
    rows = transform_clickstream(spark.createDataFrame(data, CLICKSTREAM_SCHEMA)).orderBy("timestamp").collect()
    assert len(rows) == 2
    assert [r.event_date for r in rows] == [date(2023, 10, 31), date(2023, 11, 1)]
