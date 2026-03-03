import operator as op
from collections.abc import Callable
from functools import reduce
from operator import and_, or_

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator
from pysparky.enabler import ensure_column
from pysparky.typing import ColumnOrName


def condition_and(*conditions: ColumnOrName) -> Column:
    """
    Combines multiple conditions using logical AND.

    Args:
        *conditions (ColumnOrName): Multiple PySpark Column objects or SQL expression strings representing conditions.

    Returns:
        Column: A single PySpark Column object representing the combined condition.

    Example:
        ```python
        >>> condition_and(F.col('col1') > 1, F.col('col2') < 5)
        Column<'((col1 > 1) AND (col2 < 5))'>

        >>> condition_and(F.col('col1') > 1, "col2 < 5")
        Column<'((col1 > 1) AND (col2 < 5))'>
        ```
    """
    parsed_conditions = [
        F.expr(cond) if isinstance(cond, str) else cond for cond in conditions
    ]
    return reduce(and_, parsed_conditions, F.lit(True))


def condition_or(*conditions: ColumnOrName) -> Column:
    """
    Combines multiple conditions using logical OR.

    Args:
        *conditions (ColumnOrName): Multiple PySpark Column objects or SQL expression strings representing conditions.

    Returns:
        Column: A single PySpark Column object representing the combined condition.

    Example:
        ```python
        >>> condition_or(F.col('col1') > 1, F.col('col2') < 5)
        Column<'((col1 > 1) OR (col2 < 5))'>

        >>> condition_or(F.col('col1') > 1, "col2 < 5")
        Column<'((col1 > 1) OR (col2 < 5))'>
        ```
    """
    parsed_conditions = [
        F.expr(cond) if isinstance(cond, str) else cond for cond in conditions
    ]
    return reduce(or_, parsed_conditions, F.lit(False))


def is_n_character_only(column_or_name: ColumnOrName, n: int) -> Column:
    """
    Checks if the given column or string contains exactly `n` alphabetic characters.

    Args:
        column_or_name (Column): The column or string to be checked.
        n (int): The exact number of alphabetic characters to match.

    Returns:
        Column: A column of boolean values indicating whether each entry matches the regular expression.

    Example:
        ```python
        >>> df = spark.createDataFrame([("a",), ("ab",), ("abc",), ("12",)], ["value"])
        >>> df.select(is_n_character_only(df["value"], 2).alias("is_two_char")).show()
        +-----------+
        |is_two_char|
        +-----------+
        |      false|
        |       true|
        |      false|
        |      false|
        +-----------+
        ```
    """
    # double curly braces {{ }} to escape the braces in the f-string
    regexp = rf"^[a-zA-Z]{{{n}}}$"
    return F.regexp_like(column_or_name, regexp=F.lit(regexp))


def is_two_character_only(column_or_name: ColumnOrName) -> Column:
    """
    Checks if the given column or string contains exactly two alphabetic characters (either lowercase or uppercase).

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A boolean column indicating whether the input matches the pattern of exactly two alphabetic characters.

    Example:
        ```python
        >>> df = spark.createDataFrame([("aa",), ("ZZ",), ("a1",), ("abc",)], ["value"])
        >>> df.select(is_two_character_only(df["value"]).alias("is_two_char")).show()
        +-----------+
        |is_two_char|
        +-----------+
        |       true|
        |       true|
        |      false|
        |      false|
        +-----------+
        ```
    """
    return is_n_character_only(column_or_name, n=2)


def is_all_numbers_only(column_or_name) -> Column:
    """
    Checks if the given column or string contains only numeric characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A column of boolean values indicating whether each entry contains only numeric characters.

    Example:
        ```python
        >>> df = spark.createDataFrame([("123",), ("4567",), ("89a",), ("",), ("0",)], ["value"])
        >>> df.select(is_all_numbers_only(df["value"]).alias("is_all_numbers")).show()
        +--------------+
        |is_all_numbers|
        +--------------+
        |          true|
        |          true|
        |         false|
        |         false|
        |          true|
        +--------------+
        ```
    """
    return is_n_numbers_only(column_or_name, n="+")


def is_n_numbers_only(column_or_name: ColumnOrName, n: int | str) -> Column:
    """
    Checks if the given column or string contains exactly `n` numeric characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.
        n (int | str): The exact number of numeric characters to match. or "+" for any length number.

    Returns:
        Column: A column of boolean values indicating whether each entry matches the regular expression.

    Example:
        ```python
        >>> df = spark.createDataFrame([("123",), ("4567",), ("89a",), ("",), ("0",)], ["value"])
        >>> df.select(is_n_numbers_only(df["value"], 3).alias("is_n_numbers")).show()
        +------------+
        |is_n_numbers|
        +------------+
        |        true|
        |       false|
        |       false|
        |       false|
        |       false|
        +------------+
        ```
    """
    if isinstance(n, int):
        # double curly braces {{ }} to escape the braces in the f-string
        regexp = rf"^\d{{{n}}}$"
    elif n == "+":
        # Any length number
        regexp = r"^\d+$"
    else:
        raise ValueError(
            "The parameter 'n' must be either an integer or the string '+'."
        )
    return F.regexp_like(column_or_name, F.lit(regexp))


def is_printable_only(column_or_name: ColumnOrName) -> Column:
    """
    Checks if the given column or string contains only printable characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A column of boolean values indicating whether each entry contains only printable characters.

    Example:
        ```python
        >>> df = spark.createDataFrame([("Hello!",), ("World",), ("123",), ("",), ("Non-printable\\x01",)], ["value"])
        >>> df.select(is_printable_only(df["value"]).alias("is_printable")).show()
        +------------+
        |is_printable|
        +------------+
        |        true|
        |        true|
        |        true|
        |       false|
        |       false|
        +------------+
        ```
    """
    # Regular expression for printable ASCII characters (0x20 to 0x7E)
    regexp = r"^[\x20-\x7E]+$"
    return F.regexp_like(column_or_name, F.lit(regexp))


@decorator.extension_enabler(Column)
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

    Example:
        ```python
        >>> df = spark.createDataFrame([("apple",), ("banana",), ("cherry",)], ["fruit"])
        >>> df.select("fruit", startswiths(F.col("fruit"), ["ap", "ch"]).alias("starts_with")).show()
        +------+-----------+
        | fruit|starts_with|
        +------+-----------+
        | apple|       true|
        |banana|      false|
        |cherry|       true|
        +------+-----------+
        ```
    """
    (column,) = ensure_column(column_or_name)

    return reduce(
        or_,
        map(column.startswith, list_of_strings),
        F.lit(False),
    ).alias(f"startswiths_len{len(list_of_strings)}")


def is_array_monotonic(col: ColumnOrName, cmp_fn: Callable) -> Column:
    """
    Check if an array column is monotonic according to a comparator.

    Args:
        col (ColumnOrName): Array column to check
        cmp_fn (callable): Binary function (x, y) -> Column[bool]-like.
            Typical choices:
                operator.lt  # strictly increasing
                operator.le  # non-decreasing
                operator.gt  # strictly decreasing
                operator.ge  # non-increasing

    Returns:
        Column: Boolean column: True if all adjacent pairs satisfy cmp_fn, False otherwise. Empty / single-element arrays return True.

    Example:
        ```python
        >>> import operator as op
        >>> df = spark.createDataFrame([([1, 2, 3],), ([3, 2, 1],)], ["arr"])
        >>> df.select(is_array_monotonic(F.col("arr"), op.lt).alias("is_strictly_inc")).show()
        +---------------+
        |is_strictly_inc|
        +---------------+
        |           true|
        |          false|
        +---------------+
        ```
    """
    (col_obj,) = ensure_column(col)

    # For arrays of size 0 or 1, treat as True
    return F.when(F.size(col_obj) <= 1, F.lit(True)).otherwise(
        F.aggregate(
            F.zip_with(
                F.slice(col_obj, 1, F.size(col_obj) - 1),  # a[0..n-2]
                F.slice(col_obj, 2, F.size(col_obj) - 1),  # a[1..n-1]
                cmp_fn,  # compare adjacent pairs
            ),
            F.lit(True),
            lambda acc, x: acc & x,  # AND over all comparisons
        )
    )


def is_array_strictly_increasing(col: ColumnOrName) -> Column:
    """
    Check if an array column is strictly increasing.

    Args:
        col (ColumnOrName): Array column to check

    Returns:
        Column: Boolean column: True if the array is strictly increasing, False otherwise.

    Example:
        ```python
        >>> df = spark.createDataFrame([([1, 2, 3],), ([1, 2, 2],)], ["arr"])
        >>> df.select(is_array_strictly_increasing(F.col("arr")).alias("is_inc")).show()
        +------+
        |is_inc|
        +------+
        |  true|
        | false|
        +------+
        ```
    """
    return is_array_monotonic(col, op.lt)


def is_array_non_decreasing(col: ColumnOrName) -> Column:
    """
    Check if an array column is non-decreasing.

    Args:
        col (ColumnOrName): Array column to check

    Returns:
        Column: Boolean column: True if the array is non-decreasing, False otherwise.

    Example:
        ```python
        >>> df = spark.createDataFrame([([1, 2, 2],), ([3, 2, 1],)], ["arr"])
        >>> df.select(is_array_non_decreasing(F.col("arr")).alias("is_non_dec")).show()
        +----------+
        |is_non_dec|
        +----------+
        |      true|
        |     false|
        +----------+
        ```
    """
    return is_array_monotonic(col, op.le)


def is_array_strictly_decreasing(col: ColumnOrName) -> Column:
    """
    Check if an array column is strictly decreasing.

    Args:
        col (ColumnOrName): Array column to check

    Returns:
        Column: Boolean column: True if the array is strictly decreasing, False otherwise.

    Example:
        ```python
        >>> df = spark.createDataFrame([([3, 2, 1],), ([3, 2, 2],)], ["arr"])
        >>> df.select(is_array_strictly_decreasing(F.col("arr")).alias("is_dec")).show()
        +------+
        |is_dec|
        +------+
        |  true|
        | false|
        +------+
        ```
    """
    return is_array_monotonic(col, op.gt)


def is_array_non_increasing(col: ColumnOrName) -> Column:
    """
    Check if an array column is non-increasing.

    Args:
        col (ColumnOrName): Array column to check

    Returns:
        Column: Boolean column: True if the array is non-increasing, False otherwise.

    Example:
        ```python
        >>> df = spark.createDataFrame([([3, 2, 2],), ([1, 2, 3],)], ["arr"])
        >>> df.select(is_array_non_increasing(F.col("arr")).alias("is_non_inc")).show()
        +----------+
        |is_non_inc|
        +----------+
        |      true|
        |     false|
        +----------+
        ```
    """
    return is_array_monotonic(col, op.ge)
