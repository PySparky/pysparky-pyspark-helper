from typing import Dict
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

def distinct_value_counts_map(sdf: DataFrame, column_name: str) -> DataFrame:
    """
    Get distinct values from a DataFrame column as a map with their counts.

    Args:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        DataFrame: A DataFrame containing a single column with a map of distinct values and their counts.

    Example:
        ```python
        >>> data = [("Alice",), ("Bob",), ("Alice",), ("Eve",), (None,)]
        >>> sdf = spark.createDataFrame(data, ["name"])
        >>> result = distinct_value_counts_map(sdf, "name")
        >>> result.show(truncate=False)
        +-------------------------------------------+
        |name_map                                   |
        +-------------------------------------------+
        |{Alice -> 2, Bob -> 1, Eve -> 1, NONE -> 1}|
        +-------------------------------------------+
        ```
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

def agg_apply(df: DataFrame, agg_exprs: Dict[str, Column]) -> DataFrame:
    """Apply aggregation expressions and return a DataFrame.

    Args:
        df (DataFrame): The input Spark DataFrame.
        agg_exprs (Dict[str, Column]): A dictionary where keys are the aliases for the aggregated columns
                                       and values are the Spark aggregation expressions (Columns).

    Returns:
        DataFrame: A DataFrame containing the aggregated results.

    Example:
        ```python
        >>> df = spark.createDataFrame([(1, "A"), (2, "A"), (3, "B")], ["value", "category"])
        >>> agg_exprs = {
        ...     "total_value": F.sum("value"),
        ...     "max_value": F.max("value"),
        ...     "count": F.count("*")
        ... }
        >>> result = agg_apply(df, agg_exprs)
        >>> result.show()
        +-----------+---------+-----+
        |total_value|max_value|count|
        +-----------+---------+-----+
        |          6|        3|    3|
        +-----------+---------+-----+
        ```
    """
    return df.agg(*[expr.alias(name) for name, expr in agg_exprs.items()])

def order_and_aggregate_events(
    df: DataFrame,
    id_col: str,
    value_col: str,
    record_no_col: str,
    values_array_col: str,
    record_nos_array_col: str,
    sort_asc: bool = True,
) -> DataFrame:
    """
    Group events by id, order by record_no, and aggregate into arrays.

    Args:
        df (DataFrame): The input DataFrame.
        id_col (str): The column name to group events by.
        value_col (str): The column name for values to aggregate.
        record_no_col (str): The column name for record numbers to sort by.
        values_array_col (str): The column name for the aggregated values array.
        record_nos_array_col (str): The column name for the aggregated record numbers array.
        sort_asc (bool, optional): Whether to sort the arrays in ascending order. Defaults to True.

    Returns:
        DataFrame: A DataFrame with events aggregated into arrays and ordered by record_no.

    Example:
        ```python
        >>> data = [
        ...     ("A", "val1", 2),
        ...     ("A", "val2", 1),
        ...     ("B", "val3", 1),
        ... ]
        >>> df = spark.createDataFrame(data, ["id", "value", "record_no"])
        >>> result = order_and_aggregate_events(df, "id", "value", "record_no", "values", "record_nos")
        >>> result.show(truncate=False)
        +---+------------+----------+
        |id |values      |record_nos|
        +---+------------+----------+
        |A  |[val2, val1]|[1, 2]    |
        |B  |[val3]      |[1]       |
        +---+------------+----------+
        ```
    """
    # Struct: (record_no, value) so sort_array orders by record_no
    events_df = (
        df.groupBy(F.col(id_col))
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col(record_no_col).alias("record_no"),
                        F.col(value_col).alias("value"),
                    )
                ),
                asc=sort_asc,
            ).alias("_events")  # temporary array<struct<record_no, value>>
        )
    )

    # Extract ordered arrays using transform, Python-native (no expr)
    result_df = (
        events_df
        .select(
            F.col(id_col),
            F.transform(
                F.col("_events"),
                lambda x: x["value"],
            ).alias(values_array_col),
            F.transform(
                F.col("_events"),
                lambda x: x["record_no"],
            ).alias(record_nos_array_col),
        )
    )

    return result_df
