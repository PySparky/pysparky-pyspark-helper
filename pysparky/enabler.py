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
