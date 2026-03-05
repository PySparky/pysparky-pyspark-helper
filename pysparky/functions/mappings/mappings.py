import functools
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.utils_ import utils
from pysparky.core.enabler import ensure_column
from pysparky.core.typing import ColumnOrName


def get_value_from_map(column_or_name: ColumnOrName, dict_: dict) -> Column:
    """
    Retrieves a value from a map (dictionary) using a key derived from a specified column in a DataFrame.

    This function creates a map from the provided dictionary and then looks up the value in the map
    corresponding to the key that matches the value in the specified column.

    Args:
        column_or_name (str): The name of the column in the DataFrame whose value will be used as the key to look up in the map.
        dict_ (dict): A dictionary where keys and values are the elements to be used in the map.

    Returns:
        Column: A PySpark Column object representing the value retrieved from the map.

    Example:
        ```python
        >>> map = {1: 'a', 2: 'b'}
        >>> column_name = 'key_column'
        >>> df = spark.createDataFrame([(1,), (2,)], ['key_column'])
        >>> df.withColumn('value', get_value_from_map(map, column_name)).show()
        +----------+-----+
        |key_column|value|
        +----------+-----+
        |         1|    a|
        |         2|    b|
        +----------+-----+
        ```
    """
    (column,) = ensure_column(column_or_name)

    return utils.create_map_from_dict(dict_)[column]


def when_mapping(column_or_name: ColumnOrName, dict_: dict) -> Column:
    """
    Applies a series of conditional mappings to a PySpark Column based on a dictionary of conditions and values.

    Args:
        column (Column): The PySpark Column to which the conditional mappings will be applied.
        dict_ (Dict): A dictionary where keys are the conditions and values are the corresponding results.

    Returns:
        Column: A new PySpark Column with the conditional mappings applied.

    Example:
        ```python
        >>> df = spark.createDataFrame([("A",), ("B",), ("C",)], ["category"])
        >>> mapping = {"A": 1, "B": 2}
        >>> df.select("category", when_mapping(F.col("category"), mapping).alias("mapped")).show()
        +--------+------+
        |category|mapped|
        +--------+------+
        |       A|     1|
        |       B|     2|
        |       C|  null|
        +--------+------+
        ```
    """
    (column,) = ensure_column(column_or_name)

    def reducer(result_column: Column, condition_value: tuple[Any, Any]) -> Column:
        condition, value = condition_value
        return result_column.when(column == condition, value)

    result_column: Column = functools.reduce(reducer, dict_.items(), F)  # type: ignore
    return result_column
