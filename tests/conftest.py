import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
