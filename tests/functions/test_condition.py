import pytest
from pyspark.sql import functions as F

# Assuming the functions are defined in a module named conditions
from pysparky import functions as F_


def test_is_condition_and_with_columns(spark):
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


def test_is_condition_and_with_strings(spark):
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


def test_is_condition_or_with_columns(spark):
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


def test_is_condition_or_with_strings(spark):
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


def test_is_n_character_only(spark):
    data = [("abc",), ("abcd",), ("ab",), ("a1",), ("",), ("abcde",)]
    df = spark.createDataFrame(data, ["value"])

    # Test for n = 3
    result_df = df.withColumn(
        "is_three_char", F_.is_n_character_only(F.col("value"), 3)
    )
    expected_data = [
        ("abc", True),
        ("abcd", False),
        ("ab", False),
        ("a1", False),
        ("", False),
        ("abcde", False),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_three_char"])

    assert result_df.collect() == expected_df.collect()

    # Test for n = 4
    result_df = df.withColumn("is_four_char", F_.is_n_character_only(F.col("value"), 4))
    expected_data = [
        ("abc", False),
        ("abcd", True),
        ("ab", False),
        ("a1", False),
        ("", False),
        ("abcde", False),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_four_char"])

    assert result_df.collect() == expected_df.collect()


def test_is_two_character_only(spark):
    data = [
        ("aa", True),
        ("ZZ", True),
        ("a1", False),
        ("abc", False),
        ("A", False),
        ("", False),
        (None, None),
    ]
    df = spark.createDataFrame(data, ["value", "output"])

    result_df = df.select(
        "value", F_.is_two_character_only(F.col("value")).alias("output")
    )

    assert df.collect() == result_df.collect()


def test_is_all_numbers_string(spark):
    data = [("123",), ("4567",), ("89a",), ("",), ("0",), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn("is_all_numbers", F_.is_all_numbers_only(F.col("value")))
    expected_data = [
        ("123", True),
        ("4567", True),
        ("89a", False),
        ("", False),
        ("0", True),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_all_numbers_integer(spark):
    data = [(123,), (4567,), (890,), (0,), (1234567890,), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn("is_all_numbers", F_.is_all_numbers_only(F.col("value")))
    expected_data = [
        (123, True),
        (4567, True),
        (890, True),
        (0, True),
        (1234567890, True),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_all_numbers_float(spark):
    data = [(123.45,), (6789.01,), (234.56,), (0.0,), (123456.789,), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn("is_all_numbers", F_.is_all_numbers_only(F.col("value")))
    expected_data = [
        (123.45, False),
        (6789.01, False),
        (234.56, False),
        (0.0, False),
        (123456.789, False),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_n_numbers_string(spark):
    data = [("12",), ("4567",), ("89a",), ("00",), ("0",), ("",), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn(
        "is_all_numbers", F_.is_n_numbers_only(F.col("value"), n=2)
    )
    expected_data = [
        ("12", True),
        ("4567", False),
        ("89a", False),
        ("00", True),
        ("0", False),
        ("", False),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_n_numbers_integer(spark):
    data = [(12,), (4567,), (89,), (00,), (0,), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn(
        "is_all_numbers", F_.is_n_numbers_only(F.col("value"), n=2)
    )
    expected_data = [
        (12, True),
        (4567, False),
        (89, True),
        (00, False),
        (0, False),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_n_numbers_float(spark):
    data = [(12.2,), (4567.1,), (89.0,), (00.0,), (0.0,), (None,)]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn(
        "is_all_numbers", F_.is_n_numbers_only(F.col("value"), n=2)
    )
    expected_data = [
        (12.2, False),
        (4567.1, False),
        (89.0, False),
        (00.0, False),
        (0.0, False),
        (None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_all_numbers"])

    assert result_df.collect() == expected_df.collect()


def test_is_printable_only(spark):
    data = [
        ("Hello!",),
        ("World",),
        ("123",),
        ("",),
        ("Non-printable\x01",),
        ("Printable~",),
    ]
    df = spark.createDataFrame(data, ["value"])

    result_df = df.withColumn("is_printable", F_.is_printable_only(F.col("value")))
    expected_data = [
        ("Hello!", True),
        ("World", True),
        ("123", True),
        ("", False),
        ("Non-printable\x01", False),
        ("Printable~", True),
    ]
    expected_df = spark.createDataFrame(expected_data, ["value", "is_printable"])

    assert result_df.collect() == expected_df.collect()


if __name__ == "__main__":
    pytest.main()
