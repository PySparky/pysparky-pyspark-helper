import pytest

from pysparky.transformations.sets import sets as te_sets


def test_get_unique_values(spark):
    """Test the get_unique_values function."""
    # Create sample DataFrames
    df1 = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
    df2 = spark.createDataFrame([(3,), (4,), (5,)], ["value"])

    # Expected result
    expected_data = [(1,), (2,), (3,), (4,), (5,)]
    expected_df = spark.createDataFrame(expected_data, ["value"])

    # Call the function
    result_df = te_sets.get_unique_values(df1, df2, "value")

    # Collect the results for comparison
    result_data = result_df.collect()
    expected_data = expected_df.collect()

    # Assert that the result matches the expected data
    assert sorted(result_data) == sorted(expected_data)

if __name__ == "__main__":
    pytest.main([__file__])
