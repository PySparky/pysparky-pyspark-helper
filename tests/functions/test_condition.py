import pytest
from pyspark.sql import functions as F

# Assuming the functions are defined in a module named conditions
from pysparky import functions as F_


def test_condition_and_with_columns(spark):
    df = spark.createDataFrame(
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ],
        ["col1", "col2"],
    )

    result = df.filter(F_.condition_and(F.col("col1") > 1, F.col("col2") < 5)).collect()
    assert len(result) == 1
    assert result == [(3, 4)]


def test_condition_and_with_strings(spark):
    df = spark.createDataFrame(
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ],
        ["col1", "col2"],
    )

    result = df.filter(F_.condition_and("col1 > 1", "col2 < 5")).collect()
    assert len(result) == 1
    assert result == [(3, 4)]


def test_condition_or_with_columns(spark):
    df = spark.createDataFrame(
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ],
        ["col1", "col2"],
    )

    result = df.filter(F_.condition_or(F.col("col1") < 2, F.col("col2") > 5)).collect()
    assert len(result) == 2
    assert result == [(1, 2), (5, 6)]


def test_condition_or_with_strings(spark):
    df = spark.createDataFrame(
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ],
        ["col1", "col2"],
    )

    result = df.filter(F_.condition_or("col1 < 2", "col2 > 5")).collect()
    assert len(result) == 2
    assert result == [(1, 2), (5, 6)]


if __name__ == "__main__":
    pytest.main()
