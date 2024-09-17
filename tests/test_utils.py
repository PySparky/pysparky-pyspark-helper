import pytest
from pyspark.sql import functions as F

from pysparky import utils


def test_create_map_from_dict(spark):
    # Define the dictionary
    dict_ = {"a": 1, "b": 2}

    # Create the map column using the function
    map_column = utils.create_map_from_dict(dict_)

    # Create a DataFrame to test the map column
    df = spark.createDataFrame([("a",), ("b",)], ["key_column"])

    # Add the map column to the DataFrame
    df = df.withColumn("value", map_column[F.col("key_column")])

    # Collect the results
    result = df.collect()

    # Define the expected results
    expected = [("a", 1), ("b", 2)]

    # Assert the results
    assert result == expected


if __name__ == "__main__":
    pytest.main([__file__])
