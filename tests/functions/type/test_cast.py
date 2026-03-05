from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pysparky import functions as F_


def test_cast_string_to_boolean(spark):
    data = [
        ("True", True),
        ("true", True),
        ("T", True),
        ("t", True),
        ("1", True),
        ("False", False),
        ("false", False),
        ("F", False),
        ("f", False),
        ("0", False),
        ("unknown", None),
        ("", None),
        (None, None),
    ]

    schema = T.StructType(
        [
            T.StructField("input", T.StringType(), True),
            T.StructField("expected", T.BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)
    result_df = df.withColumn("output", F_.cast_string_to_boolean(F.col("input")))
    result_with_column_name_df = df.withColumn(
        "output", F_.cast_string_to_boolean("input")
    )

    result = result_df.select("output").collect()
    result_with_column_name = result_with_column_name_df.select("output").collect()
    expected = df.select("expected").collect()

    assert result == expected
    assert result_with_column_name == expected


def test_to_timestamps_ansi_disable(spark):
    spark.conf.set("spark.sql.ansi.enabled", "false")

    date_formats = [
        "yyyy-MM-dd'T'HH:mm:ss:SSSZ",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    ]

    data = [
        ("2025-01-01T01:01:01:001+0000", datetime(2025, 1, 1, 1, 1, 1, 1000)),
        ("2025-01-01T01:01:01.001Z", datetime(2025, 1, 1, 1, 1, 1, 1000)),
        ("2025-01-01T01:01:01.001+0000", datetime(2025, 1, 1, 1, 1, 1, 1000)),
        ("I am not a date at all", None),
    ]

    schema = T.StructType(
        [
            T.StructField("input", T.StringType(), True),
            T.StructField("expected", T.TimestampType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)
    result_df = df.withColumn("output", F_.to_timestamps(F.col("input"), date_formats))
    result2_df = df.withColumn("output", F_.to_timestamps("input", date_formats))

    assert (
        result_df.select("output").collect() == result_df.select("expected").collect()
    )
    assert (
        result2_df.select("output").collect() == result2_df.select("expected").collect()
    )


def test_to_timestamps_ansi_enabled_all_valid(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")

    date_formats = [
        "yyyy-MM-dd'T'HH:mm:ss:SSSZ",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    ]

    data = [
        ("2025-01-01T01:01:01:001+0000", datetime(2025, 1, 1, 1, 1, 1, 1000)),
        ("2025-01-01T01:01:01.001Z", datetime(2025, 1, 1, 1, 1, 1, 1000)),
        ("2025-01-01T01:01:01.001+0000", datetime(2025, 1, 1, 1, 1, 1, 1000)),
    ]

    schema = T.StructType(
        [
            T.StructField("input", T.StringType(), True),
            T.StructField("expected", T.TimestampType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)
    result_df = df.withColumn("output", F_.to_timestamps(F.col("input"), date_formats))
    result2_df = df.withColumn("output", F_.to_timestamps("input", date_formats))

    assert (
        result_df.select("output").collect() == result_df.select("expected").collect()
    )
    assert (
        result2_df.select("output").collect() == result2_df.select("expected").collect()
    )

    # set it back to the default
    spark.conf.set("spark.sql.ansi.enabled", "false")


def test_to_timestamps_ansi_enabled_invalid(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")

    date_formats = [
        "yyyy-MM-dd'T'HH:mm:ss:SSSZ",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    ]

    data = [
        ("I am not a date",),
    ]

    schema = T.StructType(
        [
            T.StructField("input", T.StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    with pytest.raises(Exception):
        df.select(F_.to_timestamps(F.col("input"), date_formats)).collect()

    # set it back to the default
    spark.conf.set("spark.sql.ansi.enabled", "false")


if __name__ == "__main__":
    pytest.main()
