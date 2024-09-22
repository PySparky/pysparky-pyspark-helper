from functools import reduce
from operator import and_, or_
from typing import Callable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pysparky import decorator


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

    Example:
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
        is_desc (bool, optional): Determines the sorting order. Set to True for descending (default) or False for ascending.
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
