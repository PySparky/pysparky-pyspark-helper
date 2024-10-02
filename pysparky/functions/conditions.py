from functools import reduce
from operator import and_, or_
from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F


def condition_and(*conditions: Union[Column, str]) -> Column:
    """
    Combines multiple conditions using logical AND.

    Args:
        *conditions (Union[Column, str]): Multiple PySpark Column objects or SQL expression strings representing conditions.

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


def condition_or(*conditions: Union[Column, str]) -> Column:
    """
    Combines multiple conditions using logical OR.

    Args:
        *conditions (Union[Column, str]): Multiple PySpark Column objects or SQL expression strings representing conditions.

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
