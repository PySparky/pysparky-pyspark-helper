from functools import reduce
from operator import and_, or_

from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.typing import ColumnOrName


def condition_and(*conditions: ColumnOrName) -> Column:
    """
    Combines multiple conditions using logical AND.

    Args:
        *conditions (ColumnOrName): Multiple PySpark Column objects or SQL expression strings representing conditions.

    Returns:
        Column: A single PySpark Column object representing the combined condition.

    Examples:
        >>> condition_and(F.col('col1') > 1, F.col('col2') < 5)
        Column<'((col1 > 1) AND (col2 < 5))'>

        >>> condition_and(F.col('col1') > 1, "col2 < 5")
        Column<'((col1 > 1) AND (col2 < 5))'>
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

    Examples:
        >>> condition_or(F.col('col1') > 1, F.col('col2') < 5)
        Column<'((col1 > 1) OR (col2 < 5))'>

        >>> condition_or(F.col('col1') > 1, "col2 < 5")
        Column<'((col1 > 1) OR (col2 < 5))'>
    """
    parsed_conditions = [
        F.expr(cond) if isinstance(cond, str) else cond for cond in conditions
    ]
    return reduce(or_, parsed_conditions, F.lit(False))


def n_character_only(column_or_name: ColumnOrName, n: int) -> Column:
    """
    Checks if the given column or string contains exactly `n` alphabetic characters.

    Args:
        column_or_name (Column): The column or string to be checked.
        n (int): The exact number of alphabetic characters to match.

    Returns:
        Column: A column of boolean values indicating whether each entry matches the regular expression.
    """
    # double curly braces {{ }} to escape the braces in the f-string
    regexp = rf"^[a-zA-Z]{{{n}}}$"
    return F.regexp_like(column_or_name, regexp=F.lit(regexp))


def two_character_only(column_or_name: ColumnOrName) -> Column:
    """
    Checks if the given column or string contains exactly two alphabetic characters (either lowercase or uppercase).

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A boolean column indicating whether the input matches the pattern of exactly two alphabetic characters.

    Examples:
        >>> df = spark.createDataFrame([("aa",), ("ZZ",), ("a1",), ("abc",)], ["value"])
        >>> df.select(two_character_only(df["value"]).alias("is_two_char")).show()
        +-----------+
        |is_two_char|
        +-----------+
        |       true|
        |       true|
        |      false|
        |      false|
        +-----------+
    """
    return n_character_only(column_or_name, n=2)


def all_numbers_only(column_or_name) -> Column:
    """
    Checks if the given column or string contains only numeric characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A column of boolean values indicating whether each entry contains only numeric characters.

    Examples:
        >>> df = spark.createDataFrame([("123",), ("4567",), ("89a",), ("",), ("0",)], ["value"])
        >>> df.select(all_numbers(df["value"]).alias("is_all_numbers")).show()
        +-------------+
        |is_all_numbers|
        +-------------+
        |         true|
        |         true|
        |        false|
        |        false|
        |         true|
        +-------------+
    """
    return n_numbers_only(column_or_name, n="+")


def n_numbers_only(column_or_name: ColumnOrName, n: int | str) -> Column:
    """
    Checks if the given column or string contains exactly `n` numeric characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.
        n (int | str): The exact number of numeric characters to match.

    Returns:
        Column: A column of boolean values indicating whether each entry matches the regular expression.

    Examples:
        >>> df = spark.createDataFrame([("123",), ("4567",), ("89a",), ("",), ("0",)], ["value"])
        >>> df.select(n_numbers_only(df["value"], 3).alias("is_n_numbers")).show()
        +-------------+
        |is_n_numbers |
        +-------------+
        |         true|
        |        false|
        |        false|
        |        false|
        |        false|
        +-------------+
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


def printable_only(column_or_name: ColumnOrName) -> Column:
    """
    Checks if the given column or string contains only printable characters.

    Args:
        column_or_name (ColumnOrName): The column or string to be checked.

    Returns:
        Column: A column of boolean values indicating whether each entry contains only printable characters.

    Examples:
        >>> df = spark.createDataFrame([("Hello!",), ("World",), ("123",), ("",), ("Non-printable\x01",)], ["value"])
        >>> df.select(printable_only(df["value"]).alias("is_printable")).show()
        +-------------+
        |is_printable |
        +-------------+
        |         true|
        |         true|
        |         true|
        |        false|
        |        false|
        +-------------+
    """
    # Regular expression for printable ASCII characters (0x20 to 0x7E)
    regexp = r"^[\x20-\x7E]+$"
    return F.regexp_like(column_or_name, F.lit(regexp))
