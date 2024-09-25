import pytest

from pysparky.spark_ext import createDataFrame_from_dict


def test_createDataFrame_from_dict(spark):
    """
    Tests the createDataFrame_from_dict function.
    """
    data_dict = {"id": [1, 2, 3], "value": [100, 200, 300]}
    expected_data = [(1, 100), (2, 200), (3, 300)]
    expected_columns = ["id", "value"]

    result_df = spark.createDataFrame_from_dict(data_dict)

    assert result_df.collect() == expected_data
    assert result_df.columns == expected_columns


if __name__ == "__main__":
    pytest.main([__file__])
