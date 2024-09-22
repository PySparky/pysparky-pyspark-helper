from functools import reduce
from operator import and_
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

    This function takes a Spark DataFrame and a list of conditions, and returns
    a new DataFrame with all conditions applied using AND logic.

    Args:
        sdf (pyspark.sql.DataFrame): The input Spark DataFrame to be filtered.
        conditions (list[pyspark.sql.Column]): A list of Column expressions
            representing the filter conditions.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with all filter conditions applied.

    Example:
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'letter'])
        >>> conditions = [col('id') > 1, col('letter').isin(['b', 'c'])]
        >>> filtered_df = filters(df, conditions)
        >>> filtered_df.show()
        +---+------+
        | id|letter|
        +---+------+
        |  2|     b|
        |  3|     c|
        +---+------+

    Note:
        This function uses `functools.reduce` and `pyspark.sql.functions.and_`
        to combine multiple conditions efficiently.
    """
    if len(conditions) == 0:
        return sdf
    return sdf.filter(reduce(operator_, conditions))


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
