from pyspark.sql import Column
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