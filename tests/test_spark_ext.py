from unittest.mock import MagicMock

import pytest

from pysparky.spark_ext import (
    check_table_exists,
    convert_1d_list_to_dataframe,
    createDataFrame_from_dict,
)


def test_createDataFrame_from_dict(spark):
    """
    Tests the createDataFrame_from_dict function.
    """
    data_dict = {"id": [1, 2, 3], "value": [100, 200, 300]}
    expected_data = [(1, 100), (2, 200), (3, 300)]
    expected_columns = ["id", "value"]

    result_df = createDataFrame_from_dict(spark, data_dict)

    assert result_df.collect() == expected_data
    assert result_df.columns == expected_columns


def test_convert_1d_list_to_dataframe_column(spark):
    list_ = [1, 2, 3, 4]
    column_names = "ID1"
    df = convert_1d_list_to_dataframe(spark, list_, column_names, axis="column")
    expected_data = [(1,), (2,), (3,), (4,)]
    expected_df = spark.createDataFrame(expected_data, schema=[column_names])
    assert df.collect() == expected_df.collect()


def test_convert_1d_list_to_dataframe_row(spark):
    list_ = [1, 2, 3, 4]
    column_names = ["ID1", "ID2", "ID3", "ID4"]
    df = convert_1d_list_to_dataframe(spark, list_, column_names, axis="row")
    expected_data = [(1, 2, 3, 4)]
    expected_df = spark.createDataFrame(expected_data, schema=column_names)
    assert df.collect() == expected_df.collect()


def test_convert_1d_list_to_dataframe_invalid_axis(spark):
    list_ = [1, 2, 3, 4]
    column_names = ["numbers"]
    with pytest.raises(AttributeError):
        convert_1d_list_to_dataframe(spark, list_, column_names, axis="invalid")


def test_check_table_exists(spark):
    # Mock the output of the .collect() method
    mock_collect = MagicMock()
    mock_collect.return_value = [MagicMock(tableName="test_table")]

    # Patch the .collect() method in the SparkSession
    spark.sql = MagicMock(return_value=MagicMock(collect=mock_collect))

    # Test case where the table exists
    assert (
        check_table_exists(spark, "test_catalog", "test_database", "test_table") is True
    )

    # Test case where the table does not exist
    mock_collect.return_value = []
    assert (
        check_table_exists(spark, "test_catalog", "test_database", "non_existent_table")
        is False
    )


if __name__ == "__main__":
    pytest.main([__file__])
