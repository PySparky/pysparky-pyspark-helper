from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.typing import ColumnOrName


def column_or_name_enabler(*columns: ColumnOrName) -> tuple[Column, ...]:
    """
    Enables PySpark functions to accept either column names (as strings) or Column objects.

    Parameters:
    columns (ColumnOrName): Column names (as strings) or Column objects to be converted.

    Returns:
    tuple[Column]: A tuple of Column objects.

    Example:
    >>> column_or_name_enabler("col1", "col2", F.col("col3"))
    (Column<b'col1'>, Column<b'col2'>, Column<b'col3'>)
    """
    return tuple(
        map(
            lambda column: F.col(column) if isinstance(column, str) else column, columns
        )
    )


def column_name_or_column_names_enabler(
    column_names: str | list[str],
) -> list[str]:
    """
    Ensures that the input is always returned as a list of column names.

    Parameters:
    column_names (str | list[str]): A single column name (as a string) or a list of column names.

    Returns:
    list[str]: A list containing the column names.

    Example:
    >>> column_name_or_column_names_enabler("col1")
    ['col1']
    >>> column_name_or_column_names_enabler(["col1", "col2"])
    ['col1', 'col2']
    """

    column_names = [column_names] if isinstance(column_names, str) else column_names

    return column_names
