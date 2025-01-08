# from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.enabler import ensure_column
from pysparky.typing import ColumnOrName


def cast_string_to_boolean(column_or_name: ColumnOrName) -> Column:
    """
    Casts a column of string values to boolean values.

    This function converts specific string representations of boolean values
    to their corresponding boolean types. The recognized string values for
    `False` are "False", "false", "F", "f", and "0". The recognized string
    values for `True` are "True", "true", "T", "t", and "1". Any other values
    will be converted to None.

    Args:
        column (Column): The input column containing string values to be cast.

    Returns:
        Column: A column with boolean values where recognized strings are
        converted to their corresponding boolean values, and unrecognized
        strings are converted to None.
    """
    (column,) = ensure_column(column_or_name)

    false_string = ["False", "false", "F", "f", "0"]
    true_string = ["True", "true", "T", "t", "1"]

    return (
        F.when(column.isin(false_string), False)
        .when(column.isin(true_string), True)
        .otherwise(None)
    )


# def to_timestamps(column: Column, formats: list[str]) -> Column:
#     """
#     Converts a column with date/time strings into a timestamp column by trying multiple formats.

#     This function iterates over a list of date/time formats and attempts to parse the input column
#     using each format. The first format that successfully parses the value is used. If no format succeeds,
#     the result for that row is `NULL`.

#     Parameters:
#     ----------
#     column : Column
#         The input Spark column containing date/time strings to be converted to timestamp format.

#     formats : list[str]
#         A list of date/time format strings to try. Formats should follow the pattern
#         conventions of `java.text.SimpleDateFormat`, such as "yyyy-MM-dd", "MM/dd/yyyy", etc.

#     Returns:
#     -------
#     Column
#         A Spark Column of type timestamp. If none of the formats match for a row, the value will be `NULL`.
#     """
#     def reducer(acc, format):
#         return acc.when(
#             F.to_timestamp(column, format).isNotNull(), F.to_timestamp(column, format)
#         )

#     return reduce(reducer, formats, F)
