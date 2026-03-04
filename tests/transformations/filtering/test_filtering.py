import pytest
from pyspark.sql import functions as F

from pysparky.transformations.filtering import filtering as te_filtering

def test_filters(spark):
    test_data = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    df = spark.createDataFrame(test_data, ["id", "letter"])
    conditions = [F.col("id") > 2, F.col("letter").isin(["b", "c"])]

    result_df = te_filtering.filters(df, conditions)
    result = result_df.collect()

    assert len(result) == 1
    assert (3, "c") in result

    result_or_df = te_filtering.filters(df, conditions, "or")
    result = result_or_df.collect()

    assert len(result) == 3
    assert (2, "b") in result
    assert (3, "c") in result
    assert (4, "d") in result

def test_filters_empty_result(spark):
    test_data = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    df = spark.createDataFrame(test_data, ["id", "letter"])
    conditions = [F.col("id") > 10]

    result_df = te_filtering.filters(df, conditions)
    assert result_df.count() == 0

def test_filters_no_conditions(spark):
    test_data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(test_data, ["id", "letter"])

    result_df = te_filtering.filters(df, [])
    assert result_df.count() == df.count()
    assert result_df.collect() == df.collect()
