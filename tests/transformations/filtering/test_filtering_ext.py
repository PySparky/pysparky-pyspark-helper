import pytest

from pysparky.transformations.filtering import filtering as te_filtering


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

    result_df = te_filtering.set_columns_to_null_based_on_condition(
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

    result_df = te_filtering.set_columns_to_null_based_on_condition(
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
