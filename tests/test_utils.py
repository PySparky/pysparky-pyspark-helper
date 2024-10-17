import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pysparky import utils
from pysparky.spark_ext import createDataFrame_from_dict


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


def test_join_dataframes_on_column(spark):
    data1 = {"id": [1, 2, 3], "value1": [10, 20, 30]}
    data2 = {"id": [1, 2, 4], "value2": [100, 200, 400]}
    data3 = {"id": [1, 3, 5], "value3": [1000, 3000, 5000]}

    df1 = spark.createDataFrame_from_dict(data1)
    df2 = spark.createDataFrame_from_dict(data2)
    df3 = spark.createDataFrame_from_dict(data3)

    dataframes = [df1, df2, df3]

    result_df = utils.join_dataframes_on_column("id", *dataframes)
    result_data = result_df.collect()

    expected_data = [
        (1, 10, 100, 1000),
        (2, 20, 200, None),
        (3, 30, None, 3000),
        (4, None, 400, None),
        (5, None, None, 5000),
    ]

    expected_df = spark.createDataFrame(
        expected_data, ["id", "value1", "value2", "value3"]
    )
    expected_result = expected_df.collect()

    assert result_data == expected_result

    result_df = utils.join_dataframes_on_column("id", *dataframes, how="inner")
    result_data = result_df.collect()

    expected_data = [
        (1, 10, 100, 1000),
    ]

    expected_df = spark.createDataFrame(
        expected_data, ["id", "value1", "value2", "value3"]
    )
    expected_result = expected_df.collect()

    assert result_data == expected_result

    result_df = utils.join_dataframes_on_column("id", *dataframes, how="left")
    result_data = result_df.collect()

    expected_data = [
        (1, 10, 100, 1000),
        (2, 20, 200, None),
        (3, 30, None, 3000),
    ]

    expected_df = spark.createDataFrame(
        expected_data, ["id", "value1", "value2", "value3"]
    )
    expected_result = expected_df.collect()

    # order doesn't matter
    assert sorted(result_data) == sorted(expected_result)


def test_join_missing_column(spark):
    schema1 = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
        ]
    )
    schema2 = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    df1 = spark.createDataFrame([(1, "Alice")], schema1)
    df2 = spark.createDataFrame([("Bob", 25)], schema2)

    with pytest.raises(ValueError, match="Column 'id' not found in all DataFrames"):
        utils.join_dataframes_on_column("id", df1, df2)


def test_join_dataframes_on_column_no_input():
    with pytest.raises(ValueError, match="At least one DataFrame must be provided"):
        utils.join_dataframes_on_column("col")


def test_union_dataframes(spark):
    data1 = {"id": [1, 2, 3], "value": [10, 20, 30]}
    data2 = {"id": [4, 5, 6], "value": [40, 50, 60]}
    data3 = {"id": [7, 8, 9], "value": [70, 80, 90]}

    df1 = spark.createDataFrame_from_dict(data1)
    df2 = spark.createDataFrame_from_dict(data2)
    df3 = spark.createDataFrame_from_dict(data3)

    dataframes = [df1, df2, df3]

    result_df = utils.union_dataframes(*dataframes)
    result_data = result_df.collect()

    expected_data = [
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40),
        (5, 50),
        (6, 60),
        (7, 70),
        (8, 80),
        (9, 90),
    ]

    expected_df = spark.createDataFrame(expected_data, ["id", "value"])
    expected_result = expected_df.collect()

    assert result_data == expected_result


def test_union_dataframes_no_input():
    with pytest.raises(ValueError, match="At least one DataFrame must be provided"):
        utils.union_dataframes()


if __name__ == "__main__":
    pytest.main([__file__])
