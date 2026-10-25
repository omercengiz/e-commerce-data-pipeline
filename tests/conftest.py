import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pyspark-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def pipeline_directory(spark, tmp_path, monkeypatch):
    # The shared JVM retains its Hadoop working directory after Python chdir.
    filesystem = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    original = filesystem.getWorkingDirectory()
    filesystem.setWorkingDirectory(
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(str(tmp_path))
    )
    monkeypatch.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        filesystem.setWorkingDirectory(original)
