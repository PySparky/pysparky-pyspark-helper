import functools
import operator
from typing import Any

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(Column)
def _lower(col: Column) -> Column:
    """
    This serve as an easy example on how this package work
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
    column_or_name: str | Column, list_of_string: list[str]
) -> pyspark.sql.Column:
    """
    Creates a PySpark Column expression that checks if the given column starts with any of the strings in the list.

    Args:
        column_or_name (ColumnOrName): The column to check.
        list_of_string (List[str]): A list of strings to check if the column starts with.

    Returns:
        Column: A PySpark Column expression that evaluates to True if the column starts with any of the strings in the list, otherwise False.
    """
    # If we are not using the decorator
    # column_or_name = F.col(column_or_name) if isinstance(column_or_name, str) else column_or_name

    return functools.reduce(
        operator.or_,
        map(column_or_name.startswith, list_of_string),
        F.lit(False),
    ).alias(f"startswiths_len{len(list_of_string)}")

@decorator.extension_enabler(Column)
@decorator.pyspark_column_or_name_enabler("column_or_name")
def replace_strings_to_none(
    column_or_name: str | Column,
    list_of_null_string: list[str],
    customize_output: Any = None,
) -> pyspark.sql.Column:
    """
    Replaces empty string values in a column with None.
    Parameters:
    column_or_name (ColumnOrName): The name of the column to check for empty string values.

    Returns:
    Column: A Spark DataFrame column with the values replaced.
    """
    column_or_name = F.col(column_or_name) if isinstance(column_or_name, str) else column_or_name

    return F.when(column_or_name.isin(list_of_null_string), customize_output).otherwise(
        column_or_name
    )