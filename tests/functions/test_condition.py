import pytest
from pyspark.sql import functions as F

# Assuming the functions are defined in a module named conditions
from pysparky import functions as F_
from pysparky import spark_ext


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


def test_startswiths(spark):
    target = spark_ext.column_function(spark, F.lit(True)).collect()
    test1 = spark_ext.column_function(
        spark, F_.startswiths(F.lit("a12334"), ["a123", "234"])
    ).collect()
    assert target == test1


def test_is_array_monotonic(spark):
    data = [
        ([1, 2, 3], True, True, False, False),       # strictly increasing
        ([1, 2, 2], False, True, False, False),      # non-decreasing
        ([3, 2, 1], False, False, True, True),       # strictly decreasing
        ([3, 2, 2], False, False, False, True),      # non-increasing
        ([1, 3, 2], False, False, False, False),     # not monotonic
        ([], True, True, True, True),                # empty
        ([1], True, True, True, True)                # single element
    ]
    df = spark.createDataFrame(data, ["arr", "exp_inc", "exp_non_dec", "exp_dec", "exp_non_inc"])

    result_df = df.withColumn(
        "inc", F_.is_array_strictly_increasing("arr")
    ).withColumn(
        "non_dec", F_.is_array_non_decreasing("arr")
    ).withColumn(
        "dec", F_.is_array_strictly_decreasing("arr")
    ).withColumn(
        "non_inc", F_.is_array_non_increasing("arr")
    )

    results = result_df.collect()
    for row in results:
        assert row["inc"] == row["exp_inc"], f"Failed inc for {row['arr']}"
        assert row["non_dec"] == row["exp_non_dec"], f"Failed non_dec for {row['arr']}"
        assert row["dec"] == row["exp_dec"], f"Failed dec for {row['arr']}"
        assert row["non_inc"] == row["exp_non_inc"], f"Failed non_inc for {row['arr']}"


def test_is_array_monotonic_null_policies(spark):
    data = [
        # arr, forbid, ignore, allow_first, allow_last, allow_ends
        ([1, 2, None, 4], False, True, False, False, False),
        ([None, None, 1, 2], False, True, True, False, True),
        ([1, 2, None, None], False, True, False, True, True),
        ([None, 1, 2, None, None], False, True, False, False, True),
        ([None, None], False, True, True, True, True),
        ([None, 2, 1, None], False, False, False, False, False)
    ]
    df = spark.createDataFrame(data, ["arr", "forbid", "ignore", "first", "last", "ends"])

    # We will test strictly increasing (except for the last one which is not monotonic)
    # the last one ([None, 2, 1, None]) would fail increasing for all.

    result_df = df.withColumn(
        "forbid_res", F_.is_array_strictly_increasing("arr", null_policy="forbid")
    ).withColumn(
        "ignore_res", F_.is_array_strictly_increasing("arr", null_policy="ignore")
    ).withColumn(
        "first_res", F_.is_array_strictly_increasing("arr", null_policy="allow_first")
    ).withColumn(
        "last_res", F_.is_array_strictly_increasing("arr", null_policy="allow_last")
    ).withColumn(
        "ends_res", F_.is_array_strictly_increasing("arr", null_policy="allow_ends")
    )

    results = result_df.collect()
    for row in results:
        assert row["forbid_res"] == row["forbid"], f"Failed forbid for {row['arr']}"
        assert row["ignore_res"] == row["ignore"], f"Failed ignore for {row['arr']}"
        assert row["first_res"] == row["first"], f"Failed allow_first for {row['arr']}"
        assert row["last_res"] == row["last"], f"Failed allow_last for {row['arr']}"
        assert row["ends_res"] == row["ends"], f"Failed allow_ends for {row['arr']}"


if __name__ == "__main__":
    pytest.main()
