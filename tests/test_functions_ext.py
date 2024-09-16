import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F


from pysparky.functions_ext import _lower, chain, startswiths
from pysparky.spark_ext import column_function


def test_lower(spark):

    target = spark.column_function(F.lit("hello")).collect()
    test1 = spark.column_function(_lower(F.lit("HELLO"))).collect()
    test2 = spark.column_function(F.lit("HELLO")._lower()).collect()
    assert target == test1
    assert target == test2


def test_startswiths(spark):
    target = spark.column_function(F.lit(True)).collect()
    test1 = spark.column_function(
        startswiths(F.lit("a12334"), ["a123", "234"])
    ).collect()
    test2 = spark.column_function(
        F.lit("a12334").startswiths(["a123", "234"])
    ).collect()
    assert target == test1
    assert target == test2


def test_chain(spark):
    target = spark.column_function(F.lit("hello")).collect()
    test1 = spark.column_function(F.lit("HELLO").chain(F.lower)).collect()
    assert target == test1


if __name__ == "__main__":
    pytest.main()
