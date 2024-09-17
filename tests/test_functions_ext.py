import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

import pysparky.functions_ext as F_
from pysparky.functions_ext import (_lower, chain, replace_strings_to_none,
                                    startswiths)
from pysparky.spark_ext import column_function


def test_lower(spark):

    target = spark.column_function(F.lit("hello")).collect()
    test1 = spark.column_function(F_._lower(F.lit("HELLO"))).collect()
    test2 = spark.column_function(F.lit("HELLO")._lower()).collect()
    assert target == test1
    assert target == test2


def test_startswiths(spark):
    target = spark.column_function(F.lit(True)).collect()
    test1 = spark.column_function(
        F_.startswiths(F.lit("a12334"), ["a123", "234"])
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


def test_replace_strings_to_none(spark):
    target_sdf = spark.createDataFrame([("data",), (None,), (None,)], ["output"])

    test_sdf = spark.createDataFrame(
        [
            ("data",),
            ("",),
            (" ",),
        ]
    ).select(F_.replace_strings_to_none("_1", ["", " "]).alias("output"))

    test2_sdf = spark.createDataFrame(
        [
            ("data",),
            ("",),
            (" ",),
        ]
    ).select(F.col("_1").replace_strings_to_none(["", " "]).alias("output"))

    # it will raise error with assertDataFrameEqual if there is an error
    assert test_sdf.collect() == target_sdf.collect()


if __name__ == "__main__":
    pytest.main()
