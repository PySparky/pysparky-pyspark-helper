from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky.typing import ColumnOrName


def ensure_column(*columns: ColumnOrName) -> tuple[Column, ...]:
    """
    Enables PySpark functions to accept either column names (as strings) or Column objects.

    Parameters:
    columns (ColumnOrName): Column names (as strings) or Column objects to be converted.

    Returns:
    tuple[Column]: A tuple of Column objects.

    Examples:
    ``` py
    >>> ensure_column("col1", "col2", F.col("col3"))
    (Column<b'col1'>, Column<b'col2'>, Column<b'col3'>)
    ```

    """
    return tuple(
        map(
            lambda column: F.col(column) if isinstance(column, str) else column, columns
        )
    )


def ensure_list(single_or_list: Any | list[Any]) -> list[Any]:
    """
    Ensures the input is returned as a list.

    If the input is not already a list, it wraps the input in a list.
    If the input is already a list, it returns the input unchanged.

    Args:
        single_or_list (Union[Any, List[Any]]): The input which can be a single item or a list of items.

    Returns:
        List[Any]: A list containing the input item(s).

    Examples:
        ``` py
        >>> ensure_list(5)

        >>> ensure_list([1, 2, 3])
        [1, 2, 3]
        >>> ensure_list("hello")
        ["hello"]
        ```

    """
    return single_or_list if isinstance(single_or_list, list) else [single_or_list]


# def column_name_or_column_names_enabler(
#     column_names: str | list[str],
# ) -> list[str]:
#     """
#     Ensures that the input is always returned as a list of column names.

#     Parameters:
#     column_names (str | list[str]): A single column name (as a string) or a list of column names.

#     Returns:
#     list[str]: A list containing the column names.

#     Examples:
#     >>> column_name_or_column_names_enabler("col1")
#     ['col1']
#     >>> column_name_or_column_names_enabler(["col1", "col2"])
#     ['col1', 'col2']
#     """

#     column_names = [column_names] if isinstance(column_names, str) else column_names

#     return column_names
