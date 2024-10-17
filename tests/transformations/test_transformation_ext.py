import functools
from operator import and_, or_

import pytest

import pysparky.transformations as te


def test_distinct_value_counts_map(spark):
    data = [("Alice",), ("Bob",), ("Alice",), ("Eve",), (None,)]
    sample_data = spark.createDataFrame(data, ["name"])
    result = te.distinct_value_counts_map(sample_data, "name")
    expected_data = [({"Alice": 2, "Bob": 1, "Eve": 1, "NONE": 1},)]
    expected_df = spark.createDataFrame(expected_data, ["name_map"])

    result_list = result.collect()
    expected_list = expected_df.collect()

    assert result_list == expected_list


def test_get_unique_values(spark):
    """Test the get_unique_values function."""
    # Create sample DataFrames
    df1 = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
    df2 = spark.createDataFrame([(3,), (4,), (5,)], ["value"])

    # Expected result
    expected_data = [(1,), (2,), (3,), (4,), (5,)]
    expected_df = spark.createDataFrame(expected_data, ["value"])

    # Call the function
    result_df = te.get_unique_values(df1, df2, "value")

    # Collect the results for comparison
    result_data = result_df.collect()
    expected_data = expected_df.collect()

    # Assert that the result matches the expected data
    assert sorted(result_data) == sorted(expected_data)


def test_set_columns_to_null_based_on_condition(spark):
    data = [
        (1, 0, 0, 0),
        (2, 1, 1, 0),
        (3, 0, 1, 1),
        (4, 0, 0, 1),
        (5, 1, 0, 0),
    ]
    columns = ["ID", "col1", "col2", "col3"]
    df = spark.createDataFrame(data, columns)

    condition_column = "col1"
    condition_value = 1
    target_columns = ("col2", "col3")

    result_df = te.set_columns_to_null_based_on_condition(
        df, condition_column, condition_value, target_columns
    )

    expected_data = [
        (1, 0, 0, 0),
        (2, 1, None, None),
        (3, 0, 1, 1),
        (4, 0, 0, 1),
        (5, 1, None, None),
    ]
    expected_df = spark.createDataFrame(expected_data, columns)

    assert result_df.collect() == expected_df.collect()


def test_set_columns_to_null_based_on_condition_no_match(spark):
    data = [
        (1, 0, 0, 0),
        (2, 0, 1, 0),
        (3, 0, 1, 1),
        (4, 0, 0, 1),
        (5, 0, 0, 0),
    ]
    columns = ["ID", "col1", "col2", "col3"]
    df = spark.createDataFrame(data, columns)

    condition_column = "col1"
    condition_value = 1
    target_columns = ("col2", "col3")

    result_df = te.set_columns_to_null_based_on_condition(
        df, condition_column, condition_value, target_columns
    )

    expected_data = [
        (1, 0, 0, 0),
        (2, 0, 1, 0),
        (3, 0, 1, 1),
        (4, 0, 0, 1),
        (5, 0, 0, 0),
    ]
    expected_df = spark.createDataFrame(expected_data, columns)

    assert result_df.collect() == expected_df.collect()


if __name__ == "__main__":
    pytest.main([__file__])
