import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

import pysparky.functions_ext as F_
from pysparky.functions_ext import (chain, lower_, replace_strings_to_none,
                                    single_space_and_trim, startswiths)
from pysparky.spark_ext import column_function


def test_lower(spark):

    target = spark.column_function(F.lit("hello")).collect()
    test1 = spark.column_function(F_.lower_(F.lit("HELLO"))).collect()
    test2 = spark.column_function(F.lit("HELLO").lower_()).collect()
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


@pytest.mark.parametrize(
    "input_str, expected_str",
    [
        ("hello   world", "hello world"),
        ("  hello   world  ", "hello world"),
        ("hello world", "hello world"),
        ("   ", ""),
        ("", ""),
    ],
)
def test_single_space_and_trims(spark, input_str, expected_str):
    df = spark.createDataFrame([(input_str,)], ["input_col"])
    result_df = df.withColumn(
        "output_col", F_.single_space_and_trim(F.col("input_col"))
    )
    result = result_df.select("output_col").collect()[0][0]
    assert result == expected_str

    result_df = df.withColumn("output_col", F.col("input_col").single_space_and_trim())
    result = result_df.select("output_col").collect()[0][0]
    assert result == expected_str


def test_get_value_from_map(spark):
    dict_ = {1: "a", 2: "b", 3: "c"}
    input_sdf = spark.createDataFrame(data=[(1,), (2,), (3,)], schema=["key"])
    expected_sdf = spark.createDataFrame(
        data=[(1, "a"), (2, "b"), (3, "c")], schema=["key", "value"]
    )
    actual_sdf = input_sdf.withColumn(
        "value", F_.get_value_from_map(column_or_name="key", dict_=dict_)
    )

    assert expected_sdf.collect() == actual_sdf.collect()

    actual2_sdf = input_sdf.withColumn(
        "value", F_.get_value_from_map(column_or_name=F.col("key"), dict_=dict_)
    )
    assert expected_sdf.collect() == actual2_sdf.collect()


if __name__ == "__main__":
    pytest.main()
