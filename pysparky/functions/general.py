import functools
import operator
from typing import Any

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator, utils
from pysparky.enabler import column_or_name_enabler
from pysparky.typing import ColumnOrName


@decorator.extension_enabler(Column)
def lower_(col: Column) -> Column:
    """
    This serve as an easy Examples on how this package work
    """
    return F.lower(col)


@decorator.extension_enabler(Column)
def chain(self, func, *args, **kwargs) -> Column:
    """
    Applies a given function to the current Column and returns the result.

    This method allows for chaining operations on a Column object by applying
    a custom function with additional arguments. It's particularly useful for
    creating complex transformations or applying user-defined functions to a Column.

    Args:
        self (Column): The current Column object.
        func (callable): The function to apply to the Column.
        *args: Variable length argument list to pass to the function.
        **kwargs: Arbitrary keyword arguments to pass to the function.

    Returns:
        Column: A new Column object resulting from applying the function.

    Examples:
        >>> df = spark.createDataFrame([("hello",)], ["text"])
        >>> def custom_upper(col):
        ...     return F.upper(col)
        >>> result = df.withColumn("upper_text", df.text.chain(custom_upper))
        >>> result.show()
        +-----+----------+
        | text|upper_text|
        +-----+----------+
        |hello|     HELLO|
        +-----+----------+

        >>> def add_prefix(col, prefix):
        ...     return F.concat(F.lit(prefix), col)
        >>> result = df.withColumn("prefixed_text", df.text.chain(add_prefix, prefix="Pre: "))
        >>> result.show()
        +-----+-------------+
        | text|prefixed_text|
        +-----+-------------+
        |hello|   Pre: hello|
        +-----+-------------+

    Note:
        The function passed to `chain` should expect a Column as its first argument,
        followed by any additional arguments specified in the `chain` call.
    """
    return func(self, *args, **kwargs)


@decorator.extension_enabler(Column)
@decorator.pyspark_column_or_name_enabler("column_or_name")
def startswiths(
    column_or_name: ColumnOrName, list_of_strings: list[str]
) -> pyspark.sql.Column:
    """
    Creates a PySpark Column expression to check if the given column starts with any string in the list.

    Args:
        column_or_name (ColumnOrName): The column to check.
        list_of_strings (List[str]): A list of strings to check if the column starts with.

    Returns:
        Column: A PySpark Column expression that evaluates to True if the column starts with any string in the list, otherwise False.
    """

    return functools.reduce(
        operator.or_,
        map(column_or_name.startswith, list_of_strings),
        F.lit(False),
    ).alias(f"startswiths_len{len(list_of_strings)}")


@decorator.extension_enabler(Column)
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
    """

    (column,) = column_or_name_enabler(column_or_name)

    return F.when(column.isin(list_of_null_string), customize_output).otherwise(column)


@decorator.extension_enabler(Column)
def single_space_and_trim(column_or_name: ColumnOrName) -> Column:
    """
    Replaces multiple white spaces with a single space and trims the column.

    Args:
        column_or_name (Column): The column to be adjusted.

    Returns:
        Column: A trimmed column with single spaces.
    """

    return F.trim(F.regexp_replace(column_or_name, r"\s+", " "))


@decorator.extension_enabler(Column)
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

    Examples:
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
    """
    (column,) = column_or_name_enabler(column_or_name)

    return utils.create_map_from_dict(dict_)[column]


@decorator.extension_enabler(Column)
def when_mapping(column_or_name: ColumnOrName, dict_: dict) -> Column:
    """
    Applies a series of conditional mappings to a PySpark Column based on a dictionary of conditions and values.

    Args:
        column (Column): The PySpark Column to which the conditional mappings will be applied.
        dict_ (Dict): A dictionary where keys are the conditions and values are the corresponding results.

    Returns:
        Column: A new PySpark Column with the conditional mappings applied.
    """
    (column,) = column_or_name_enabler(column_or_name)

    def reducer(result_column: Column, condition_value: tuple[Any, Any]) -> Column:
        condition, value = condition_value
        return result_column.when(column == condition, value)

    result_column: Column = functools.reduce(reducer, dict_.items(), F)  # type: ignore
    return result_column
