from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pysparky import decorator, enabler


@decorator.extension_enabler(DataFrame)
def get_latest_record_from_column(
    sdf: DataFrame,
    window_partition_column_name: str,
    window_order_by_column_names: str | list[str],
    window_function: Callable = F.row_number,
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

    Example:
        ```python
        >>> data = [
        ...     ("A", 1, "2021-01-01"),
        ...     ("A", 2, "2021-01-02"),
        ...     ("B", 1, "2021-01-01")
        ... ]
        >>> df = spark.createDataFrame(data, ["id", "version", "date"])
        >>> result = get_latest_record_from_column(df, "id", "date")
        >>> result.show()
        +---+-------+----------+
        | id|version|      date|
        +---+-------+----------+
        |  A|      2|2021-01-02|
        |  B|      1|2021-01-01|
        +---+-------+----------+
        ```
    """
    window_order_by_column_names = enabler.ensure_list(window_order_by_column_names)

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


def quarantine_duplicate_record(
    sdf: DataFrame, column_name: str
) -> tuple[DataFrame, DataFrame]:
    """
    Splits the input DataFrame into two DataFrames: one containing unique records
    based on the specified column, and the other containing duplicate records.

    Args:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The column name to check for duplicates.

    Returns:
        tuple[DataFrame, DataFrame]: A tuple containing two DataFrames. The first DataFrame
        contains unique records, and the second DataFrame contains duplicate records.

    Example:
        ```python
        >>> data = [(1, "A"), (2, "B"), (3, "C"), (1, "A"), (4, "D"), (2, "B")]
        >>> sdf = spark.createDataFrame(data, ["id", "value"])
        >>> unique_records, duplicate_records = quarantine_duplicate_record(sdf, "id")
        >>> unique_records.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  3|    C|
        |  4|    D|
        +---+-----+
        >>> duplicate_records.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    A|
        |  1|    A|
        |  2|    B|
        |  2|    B|
        +---+-----+
        ```
    """
    window_spec = Window.partitionBy(column_name)
    with_count_sdf = sdf.withColumn("count", F.count(column_name).over(window_spec))
    unique_records_sdf = with_count_sdf.filter(F.col("count") == 1).drop("count")
    duplicate_records_sdf = with_count_sdf.filter(F.col("count") > 1).drop("count")

    return unique_records_sdf, duplicate_records_sdf


def get_only_duplicate_record(sdf: DataFrame, column_name: str) -> DataFrame:
    """
    Retrieves only the duplicate records from the input DataFrame
    based on the specified column.

    Args:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The column name to check for duplicates.

    Returns:
        DataFrame: A DataFrame containing only the duplicate records.

    Example:
        ```python
        >>> data = [(1, "A"), (2, "B"), (3, "C"), (1, "A"), (4, "D"), (2, "B")]
        >>> sdf = spark.createDataFrame(data, ["id", "value"])
        >>> duplicate_records = get_only_duplicate_record(sdf, "id")
        >>> duplicate_records.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    A|
        |  1|    A|
        |  2|    B|
        |  2|    B|
        +---+-----+
        ```
    """
    _, duplicate_records_sdf = quarantine_duplicate_record(sdf, column_name)
    return duplicate_records_sdf


def get_only_unique_record(sdf: DataFrame, column_name: str) -> DataFrame:
    """
    Retrieves only the unique records from the input DataFrame
    based on the specified column.

    Args:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The column name to check for duplicates.

    Returns:
        DataFrame: A DataFrame containing only the unique records.

    Example:
        ```python
        >>> data = [(1, "A"), (2, "B"), (3, "C"), (1, "A"), (4, "D"), (2, "B")]
        >>> sdf = spark.createDataFrame(data, ["id", "value"])
        >>> unique_records = get_only_unique_record(sdf, "id")
        >>> unique_records.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  3|    C|
        |  4|    D|
        +---+-----+
        ```
    """
    unique_records_sdf, _ = quarantine_duplicate_record(sdf, column_name)
    return unique_records_sdf
