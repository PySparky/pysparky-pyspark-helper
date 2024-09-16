import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark
