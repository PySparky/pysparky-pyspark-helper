from functools import reduce
from operator import and_, or_
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(DataFrame)
def apply_cols(
    sdf: DataFrame, col_func: Callable, cols: list[str] | None = None, **kwargs
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
    sdf: DataFrame, conditions: list[Column], operator_: str = "and"
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
    match operator_:
        case "and":
            operator_callable = and_
        case "or":
            operator_callable = or_
        case _:
            raise ValueError(
                f"Unsupported operator: {operator_}. Valid options are 'and' and 'or'."
            )

    default_value = F.lit(True) if operator_callable == and_ else F.lit(False)
    return sdf.filter(reduce(operator_callable, conditions, default_value))


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
        ``` py
        spark = SparkSession.builder.appName("UniqueValues").getOrCreate()
        df1 = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        df2 = spark.createDataFrame([(3,), (4,), (5,)], ["value"])
        unique_values = get_unique_values(df1, df2, "value")
        unique_values.show()
        ```
    """
    # Union the DataFrames
    union_df = df1.select(column_name).union(df2.select(column_name))

    # Perform distinct to get unique values
    unique_values_df = union_df.distinct()

    return unique_values_df


def set_columns_to_null_based_on_condition(
    df: DataFrame,
    condition_column: str,
    condition_value: str,
    target_columns: tuple[str],
) -> DataFrame:
    """
    Sets specified columns to null based on a condition in the given DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        condition_column (str): The name of the column containing the condition value.
        condition_value (str): The value indicating the condition to set columns to null.
        target_columns (Tuple[str]): The tuple of columns to be set as null if the condition value is found.

    Returns:
        DataFrame: The updated DataFrame with specified columns set to null based on the condition.

    Examples:
        ``` py
        >>> data = [
        ...     (1, 0, 0, 0),
        ...     (2, 0, 1, 0),
        ...     (3, 1, 1, 1),
        ...     (4, 1, 0, 1),
        ...     (5, 0, 0, 0),
        ... ]
        >>> columns = ["ID", "Dummy1", "Dummy2", "Dummy3"]
        >>> df = spark.createDataFrame(data, columns)
        >>> condition_column = "Dummy1"
        >>> condition_value = 1
        >>> target_columns = ("Dummy2", "Dummy3")
        >>> result_df = set_columns_to_null_based_on_condition(df, condition_column, condition_value, target_columns)
        >>> result_df.show()
        +---+------+-------+-------+
        | ID|Dummy1|Dummy2 |Dummy3 |
        +---+------+-------+-------+
        |  1|     0|      0|      0|
        |  2|     0|      1|      0|
        |  3|     1|   null|   null|
        |  4|     1|   null|   null|
        |  5|     0|      0|      0|
        +---+------+-------+-------+
        ```
    """
    return df.withColumns(
        {
            col: F.when(
                F.col(condition_column) == condition_value, F.lit(None)
            ).otherwise(F.col(col))
            for col in target_columns
        }
    )


def execute_transformation_blueprint(
    sdf: DataFrame, blueprint: dict[str, Column]
) -> DataFrame:
    """
    Executes a transformation blueprint on a Spark DataFrame.

    The transformation blueprint is a dictionary where keys are column names
    and values are the corresponding transformations to apply. The function
    applies each transformation in the order specified by the blueprint and
    returns the resulting DataFrame with the transformed columns.

    A transformation_blueprint is a dictionary that the
    key: new column name
    value: Column function

    Args:
        sdf (DataFrame): The input DataFrame to be transformed.
        blueprint (Dict[str, Column]): A dictionary of column names as keys and
            transformation functions as values.

    Returns:
        DataFrame: The resulting DataFrame with the transformed columns.

    Example:
        sdf.transform(execute_transformation_blueprint, processing_blueprint).show()
    """
    return sdf.select(
        [
            column_processing.alias(new_column_name)
            for new_column_name, column_processing in blueprint.items()
        ]
    )
