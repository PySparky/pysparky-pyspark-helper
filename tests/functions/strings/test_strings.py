import pytest
from pyspark.sql import functions as F

from pysparky.functions.strings import strings as F_strings
from pysparky.core.spark_ext import column_function


def test_lower(spark):
    target = column_function(spark, F.lit("hello")).collect()
    test1 = column_function(spark, F_strings.lower_(F.lit("HELLO"))).collect()
    assert target == test1


def test_replace_strings_to_none(spark):
    target_sdf = spark.createDataFrame([("data",), (None,), (None,)], ["output"])

    test_sdf = spark.createDataFrame(
        [
            ("data",),
            ("",),
            (" ",),
        ]
    ).select(F_strings.replace_strings_to_none("_1", ["", " "]).alias("output"))

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
        "output_col", F_strings.single_space_and_trim(F.col("input_col"))
    )
    result = result_df.select("output_col").collect()[0][0]
    assert result == expected_str


if __name__ == "__main__":
    pytest.main([__file__])
