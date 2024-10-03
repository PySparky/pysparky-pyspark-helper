from functools import reduce
from operator import and_, or_
from typing import Callable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(DataFrame)
def apply_cols(
    sdf: DataFrame, col_func: Callable, cols: list[str] = None, **kwargs
) -> DataFrame:
    """
    Apply a function to specified columns of a Spark DataFrame.

    Parameters:
        sdf (DataFrame): The input Spark DataFrame.
        col_func (callable): The function to apply to each column.
        cols (list[str], optional): List of column names to apply the function to.
                                    If None, applies to all columns. Defaults to None.

    Returns:
        DataFrame: A new Spark DataFrame with the function applied to the specified columns.
    """
    if cols is None:
        cols = sdf.columns
    return sdf.withColumns(
        {col_name: col_func(col_name, **kwargs) for col_name in cols}
    )


@decorator.extension_enabler(DataFrame)
def transforms(
    sdf: DataFrame, transformations: list[tuple[Callable, dict]]
) -> DataFrame:
    """
    Apply a series of transformations to a Spark DataFrame.

    Parameters:
        sdf (DataFrame): The input Spark DataFrame to be transformed.
        transformations (list): A list of transformations, where each transformation is a tuple
                        containing a function and a dictionary of keyword arguments to apply the function to.

    Returns:
        DataFrame: The transformed Spark DataFrame.
    """
    for transformation_funcs, kwarg in transformations:
        assert callable(transformation_funcs), "transformation_funcs must be callable"
        assert isinstance(kwarg, dict), "kwarg must be a dictionary"
        sdf = sdf.transform(transformation_funcs, **kwarg)
    return sdf


@decorator.extension_enabler(DataFrame)
def filters(
    sdf: DataFrame, conditions: list[Column], operator_: Callable = and_
) -> DataFrame:
    """
    Apply multiple filter conditions to a Spark DataFrame.

    This function takes a Spark DataFrame, a list of conditions, and an optional
    operator. It returns a new DataFrame with all conditions applied using the
    specified operator.

    Args:
        sdf (pyspark.sql.DataFrame): The input Spark DataFrame to be filtered.
        conditions (list[pyspark.sql.Column]): A list of Column expressions
            representing the filter conditions.
        operator_ (Callable, optional): The operator to use for combining
            conditions. Defaults to `and_`. Valid options are `and_` and `or_`.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with all filter conditions applied.

    Raises:
        ValueError: If an unsupported operator is provided.

    Examples:
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'letter'])
        >>> conditions = [col('id') > 1, col('letter').isin(['b', 'c'])]

        # Filter using AND (default behavior)
        >>> filtered_df_and = filters(df, conditions)
        >>> filtered_df_and.show()
        +---+------+
        | id|letter|
        +---+------+
        |  2|     b|
        |  3|     c|
        +---+------+

        # Filter using OR
        >>> filtered_df_or = filters(df, conditions, or_)
        >>> filtered_df_or.show()
        +---+------+
        | id|letter|
        +---+------+
        |  2|     b|
        |  3|     c|
        |  1|     a|
        +---+------+
    """
    if operator_ not in (and_, or_):
        raise ValueError(
            f"Unsupported operator: {operator_}. Valid options are 'and_' and 'or_."
        )

    default_value = F.lit(True) if operator_ == and_ else F.lit(False)
    return sdf.filter(reduce(operator_, conditions, default_value))


@decorator.extension_enabler(DataFrame)
def get_latest_record_from_column(
    sdf: DataFrame,
    window_partition_column_name: str,
    window_order_by_column_names: str | list,
    window_function: Column = F.row_number,
) -> DataFrame:
    """
    Fetches the most recent record from a DataFrame based on a specified column, allowing for custom sorting order.

    Parameters:
        sdf (DataFrame): The DataFrame to process.
        window_partition_column_name (str): The column used to partition the DataFrame.
        window_order_by_column_names (str | list): The column(s) used to sort the DataFrame.
        window_function (Column, optional): The window function for ranking records. Defaults to F.row_number.

    Returns:
        DataFrame: A DataFrame with the most recent record for each partition.
    """

    if not isinstance(window_order_by_column_names, list):
        window_order_by_column_names = [window_order_by_column_names]

    return (
        sdf.withColumn(
            "temp",
            window_function().over(
                Window.partitionBy(window_partition_column_name).orderBy(
                    *window_order_by_column_names
                )
            ),
        )
        .filter(F.col("temp") == 1)
        .drop("temp")
    )


@decorator.extension_enabler(DataFrame)
def distinct_value_counts_map(sdf: DataFrame, column_name: str) -> DataFrame:
    """
    Get distinct values from a DataFrame column as a map with their counts.

    Args:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        DataFrame: A DataFrame containing a single column with a map of distinct values and their counts.

    Examples:
        >>> data = [("Alice",), ("Bob",), ("Alice",), ("Eve",), (None,)]
        >>> sdf = spark.createDataFrame(data, ["name"])
        >>> result = distinct_value_counts_map(sdf, "name")
        >>> result.show(truncate=False)
        +--------------------------+
        |name_map                  |
        +--------------------------+
        |{Alice -> 2, Bob -> 1, Eve -> 1, NONE -> 1}|
        +--------------------------+
    """
    return (
        sdf.select(column_name)
        .na.fill("NONE")
        .groupBy(column_name)
        .count()
        .select(
            F.map_from_entries(F.collect_list(F.struct(column_name, "count"))).alias(
                f"{column_name}_map"
            )
        )
    )


def get_unique_values(df1: DataFrame, df2: DataFrame, column_name: str) -> DataFrame:
    """Unions two DataFrames and returns a DataFrame with unique values.

    Args:
        df1 (DataFrame): First DataFrame.
        df2 (DataFrame): Second DataFrame.
        column_name (str): The column name containing the values.

    Returns:
        DataFrame: A DataFrame with unique values.

    Examples:
        # spark = SparkSession.builder.appName("UniqueValues").getOrCreate()
        # df1 = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        # df2 = spark.createDataFrame([(3,), (4,), (5,)], ["value"])
        # unique_values = get_unique_values(df1, df2, "value")
        # unique_values.show()
    """
    # Union the DataFrames
    union_df = df1.select(column_name).union(df2.select(column_name))

    # Perform distinct to get unique values
    unique_values_df = union_df.distinct()

    return unique_values_df
