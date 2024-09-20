from typing import Sequence

from pyspark.sql import functions as F


def expect_type(col_name, col_type):
    """
    A decorator function that verifies the data type of a specified column in a Spark DataFrame.

    Parameters:
        col_name (str): The column's name.
        col_type (pyspark.sql.types.DataType): The expected data type for the column.

    Returns:
        function: A decorated function that checks the column's data type.

    Raises:
        AssertionError: If the column's data type does not match the expected type.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            source_type = spark_table_sdf.schema[col_name].dataType
            target_type = col_type
            assert (
                source_type == target_type
            ), f"Data type of column '{col_name}:{source_type}' is not equal to {target_type}"  # noqa: E501
            print(f"✅: Column '{col_name}' has the expected data type {col_type}")
            return spark_table_sdf

        return wrapper

    return decorator


def expect_unique(col_name):
    """
    A decorator function that ensures the uniqueness of a column in a Spark DataFrame.

    Parameters:
        col_name (str): The column's name.

    Returns:
        function: A decorated function that checks the column's uniqueness.

    Raises:
        AssertionError: If the column's count and distinct count are not equal.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            spark_table_col_sdf = spark_table_sdf.select(col_name)
            normal_count = spark_table_col_sdf.count()
            distinct_count = spark_table_col_sdf.distinct().count()
            assert (
                normal_count == distinct_count
            ), f"Count and distinct count of column '{col_name}' are not equal"
            print(f"✅: Column '{col_name}' is distinct")
            return spark_table_sdf

        return wrapper

    return decorator


def expect_criteria(criteria):
    """
    A decorator function that ensures a specific criterion on a Spark DataFrame.

    Parameters:
        criteria (pyspark.sql.column.Column): The filter criterion to be applied to the DataFrame.

    Returns:
        function: A decorated function that checks the criterion.

    Raises:
        AssertionError: If the filtered count and unfiltered count of the DataFrame are not equal.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            filtered_count = spark_table_sdf.filter(criteria).count()
            unfiltered_count = spark_table_sdf.count()
            assert (
                filtered_count == unfiltered_count
            ), f"Filtered count is not equal to unfiltered count {criteria}"
            print(f"✅: Criteria '{criteria}' passed")
            return spark_table_sdf

        return wrapper

    return decorator


def expect_any_to_one(col1: str | Sequence[str], col2: str | Sequence[str]):
    """
    A decorator function that ensures an N:1 relationship between col1 and col2,
    meaning each value in col1 corresponds to only one distinct value in col2.

    Args:
        col1 (str | Sequence[str]): Name of the column or a tuple of column names.
        col2 (str | Sequence[str]): Name of the column or a tuple of column names.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            num_col2_values_with_many_col1_values = (
                spark_table_sdf.groupBy(*col1)
                .agg(F.count_distinct(*col2).alias("distinct_count"))
                .where(F.col("distinct_count") > 1)
                .count()
            )
            assert (
                num_col2_values_with_many_col1_values == 0
            ), f"Multiple {col2}s per {col1}"
            print(f"✅: {col1}:{col2} is N:1")
            return spark_table_sdf

        return wrapper

    return decorator


def expect_one_to_one(col1: str | Sequence[str], col2: str | Sequence[str]):
    """
    A decorator function that ensures a 1:1 relationship between col1 and col2,
    meaning each value in col1 corresponds to only one distinct value in col2 and vice-versa.

    Args:
        col1 (str | Sequence[str]): Name of the column or a tuple of column names.
        col2 (str | Sequence[str]): Name of the column or a tuple of column names.
    """

    any_to_one = expect_any_to_one(col1, col2)
    one_to_any = expect_any_to_one(col2, col1)

    def decorator(func):
        return one_to_any(any_to_one(func))

    return decorator
