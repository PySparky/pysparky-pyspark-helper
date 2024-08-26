import functools
import operator

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator

@decorator.extension_enabler(Column)
def _lower(col:Column) -> Column:

    return F.lower(col)

@decorator.extension_enabler(Column)
@decorator.pyspark_column_or_name_enabler("column_or_name")
def startswiths(column_or_name: "ColumnOrName", list_of_string: list[str]) -> pyspark.sql.Column:
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
        map(lambda bins: column_or_name.startswith(bins), list_of_string), 
        F.lit(False)
    ).alias(f"startswiths_len{len(list_of_string)}")