import functools
from typing import Any

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.core.enabler import ensure_column
from pysparky.core.typing import ColumnOrName


def lower_(col: Column) -> Column:
    """
    This serve as an easy Examples on how this package work

    Args:
        col (Column): The column to be lowercased.

    Returns:
        Column: A lowercased column.

    Example:
        ```python
        >>> df = spark.createDataFrame([("Hello",)], ["text"])
        >>> df.select(lower_(F.col("text"))).show()
        +-----------+
        |lower(text)|
        +-----------+
        |      hello|
        +-----------+
        ```
    """
    return F.lower(col)


def replace_strings_to_none(
    column_or_name: ColumnOrName,
    list_of_null_string: list[str],
    customize_output: Any = None,
) -> pyspark.sql.Column:
    """
    Replaces empty string values in a column with None.

    Args:
        column_or_name (ColumnOrName): The column to check for empty string values.

    Returns:
        Column: A Spark DataFrame column with the values replaced.

    Example:
        ```python
        >>> df = spark.createDataFrame([("",), ("foo",), (None,)], ["col"])
        >>> df.select(replace_strings_to_none(F.col("col"), [""]).alias("cleaned")).show()
        +-------+
        |cleaned|
        +-------+
        |   null|
        |    foo|
        |   null|
        +-------+
        ```
    """

    (column,) = ensure_column(column_or_name)

    return F.when(column.isin(list_of_null_string), customize_output).otherwise(column)


def single_space_and_trim(column_or_name: ColumnOrName) -> Column:
    """
    Replaces multiple white spaces with a single space and trims the column.

    Args:
        column_or_name (Column): The column to be adjusted.

    Returns:
        Column: A trimmed column with single spaces.

    Example:
        ```python
        >>> df = spark.createDataFrame([("  foo   bar  ",)], ["text"])
        >>> df.select(single_space_and_trim(F.col("text")).alias("cleaned")).show()
        +-------+
        |cleaned|
        +-------+
        |foo bar|
        +-------+
        ```
    """

    return F.trim(F.regexp_replace(column_or_name, r"\s+", " "))
