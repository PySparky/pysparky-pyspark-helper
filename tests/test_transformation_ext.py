from operator import and_, or_
import functools

import pytest
from pyspark.sql import functions as F

from pysparky import transformation_ext as te


def test_apply_cols(spark):
    # Apply the function to the DataFrame
    data = [("1", "John", "Doe"), ("2", "Jane", "Smith")]
    df = spark.createDataFrame(data, ["id", "first_name", "last_name"])
    result_df = te.apply_cols(df, F.upper, ["first_name", "last_name"])

    # Collect the results
    result = result_df.collect()

    # Expected data
    expected_data = [("1", "JOHN", "DOE"), ("2", "JANE", "SMITH")]

    assert result == expected_data


def test_transforms(spark):
    # Define the pipeline
    upper_cols_partial = functools.partial(te.apply_cols, col_func=F.upper)
    plus_one_transformation = lambda sdf: sdf.withColumn("id", F.col("id") + 1)

    pipeline = [
        (upper_cols_partial, {"cols": ["name"]}),
        (plus_one_transformation, {}),
    ]

    data = [(1, "John"), (2, "Jane")]
    df = spark.createDataFrame(data, ["id", "name"])
    result_df = te.transforms(df, pipeline)

    # Collect the results
    result = result_df.collect()

    # Expected data
    expected_data = [(2, "JOHN"), (3, "JANE")]

    # Assert the results
    assert expected_data == result


def test_filters(spark):
    # Create a test DataFrame
    test_data = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    df = spark.createDataFrame(test_data, ["id", "letter"])

    # Define filter conditions
    conditions = [F.col("id") > 2, F.col("letter").isin(["b", "c"])]

    # Apply the filters function
    result_df = te.filters(df, conditions)

    # Convert the result to a list for easy assertion
    result = result_df.collect()

    # Assert the expected output
    assert len(result) == 1
    assert (2, "b") not in result
    assert (3, "c") in result
    assert (1, "a") not in result
    assert (4, "d") not in result

    # Apply the filters function
    result_or_df = te.filters(df, conditions, or_)

    # Convert the result to a list for easy assertion
    result = result_or_df.collect()

    # Assert the expected output
    assert len(result) == 3
    assert (2, "b") in result
    assert (3, "c") in result
    assert (1, "a") not in result
    assert (4, "d") in result


def test_filters_empty_result(spark):
    # Create a test DataFrame
    test_data = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    df = spark.createDataFrame(test_data, ["id", "letter"])

    # Define filter conditions that result in an empty DataFrame
    conditions = [F.col("id") > 10]

    # Apply the filters function
    result_df = te.filters(df, conditions)

    # Assert the result is an empty DataFrame
    assert result_df.count() == 0


def test_filters_no_conditions(spark):
    # Create a test DataFrame
    test_data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(test_data, ["id", "letter"])

    # Apply the filters function with no conditions
    result_df = te.filters(df, [])

    # Assert the result is the same as the input DataFrame
    assert result_df.count() == df.count()
    assert result_df.collect() == df.collect()


def test_get_latest_record_from_column(spark):
    input_sdf = spark.createDataFrame(
        [
            (1, "Apple", "old"),
            (2, "Apple", "new"),
            (1, "Orange", "old"),
            (2, "Orange", "old"),
            (3, "Orange", "new"),
        ],
        ["ts", "product", "property"],
    )

    output_sdf = input_sdf.transform(
        te.get_latest_record_from_column,
        window_partition_column_name="product",
        window_order_by_column_names=F.col("ts").desc(),
        window_function=F.row_number,
    )

    target_sdf = spark.createDataFrame(
        [
            (2, "Apple", "new"),
            (3, "Orange", "new"),
        ],
        ["ts", "product", "property"],
    )

    # it will raise error with assertDataFrameEqual if there is an error
    assert output_sdf.collect() == target_sdf.collect()


if __name__ == "__main__":
    pytest.main([__file__])
