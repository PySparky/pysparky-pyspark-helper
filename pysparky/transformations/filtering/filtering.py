from functools import reduce
from operator import and_, or_
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

def filters(
    sdf: DataFrame, conditions: list[Column], operator_: str = "and"
) -> DataFrame:
    """
    Apply multiple filter conditions to a Spark DataFrame.

    This function takes a Spark DataFrame, a list of conditions, and an optional
    operator. It returns a new DataFrame with all conditions applied using the
    specified operator.

    Args:
        sdf (pyspark.sql.DataFrame): The input Spark DataFrame to be filtered.
        conditions (list[pyspark.sql.Column]): A list of Column expressions
            representing the filter conditions.
        operator_ (Callable, optional): The operator to use for combining
            conditions. Defaults to `and_`. Valid options are `and_` and `or_`.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with all filter conditions applied.

    Raises:
        ValueError: If an unsupported operator is provided.

    Example:
        ```python
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'letter'])
        >>> conditions = [col('id') > 1, col('letter').isin(['b', 'c'])]

        # Filter using AND (default behavior)
        >>> filtered_df_and = filters(df, conditions)
        >>> filtered_df_and.show()
        +---+------+
        | id|letter|
        +---+------+
        |  2|     b|
        |  3|     c|
        +---+------+

        # Filter using OR
        >>> filtered_df_or = filters(df, conditions, or_)
        >>> filtered_df_or.show()
        +---+------+
        | id|letter|
        +---+------+
        |  2|     b|
        |  3|     c|
        |  1|     a|
        +---+------+
        ```
    """
    match operator_:
        case "and":
            operator_callable = and_
        case "or":
            operator_callable = or_
        case _:
            raise ValueError(
                f"Unsupported operator: {operator_}. Valid options are 'and' and 'or'."
            )

    default_value = F.lit(True) if operator_callable == and_ else F.lit(False)
    return sdf.filter(reduce(operator_callable, conditions, default_value))


def set_columns_to_null_based_on_condition(
    df: DataFrame,
    condition_column: str,
    condition_value: str,
    target_columns: tuple[str],
) -> DataFrame:
    """
    Sets specified columns to null based on a condition in the given DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        condition_column (str): The name of the column containing the condition value.
        condition_value (str): The value indicating the condition to set columns to null.
        target_columns (Tuple[str]): The tuple of columns to be set as null if the condition value is found.

    Returns:
        DataFrame: The updated DataFrame with specified columns set to null based on the condition.

    Example:
        ```python
        >>> data = [
        ...     (1, 0, 0, 0),
        ...     (2, 0, 1, 0),
        ...     (3, 1, 1, 1),
        ...     (4, 1, 0, 1),
        ...     (5, 0, 0, 0),
        ... ]
        >>> columns = ["ID", "Dummy1", "Dummy2", "Dummy3"]
        >>> df = spark.createDataFrame(data, columns)
        >>> condition_column = "Dummy1"
        >>> condition_value = 1
        >>> target_columns = ("Dummy2", "Dummy3")
        >>> result_df = set_columns_to_null_based_on_condition(df, condition_column, condition_value, target_columns)
        >>> result_df.show()
        +---+------+------+------+
        | ID|Dummy1|Dummy2|Dummy3|
        +---+------+------+------+
        |  1|     0|     0|     0|
        |  2|     0|     1|     0|
        |  3|     1|  null|  null|
        |  4|     1|  null|  null|
        |  5|     0|     0|     0|
        +---+------+------+------+
        ```
    """
    return df.withColumns(
        {
            col: F.when(
                F.col(condition_column) == condition_value, F.lit(None)
            ).otherwise(F.col(col))
            for col in target_columns
        }
    )
