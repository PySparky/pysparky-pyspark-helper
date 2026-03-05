from typing import Callable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

def apply_cols(
    sdf: DataFrame, col_func: Callable, cols: list[str] | None = None, **kwargs
) -> DataFrame:
    """
    Apply a function to specified columns of a Spark DataFrame.

    Parameters:
        sdf (DataFrame): The input Spark DataFrame.
        col_func (callable): The function to apply to each column.
        cols (list[str], optional): List of column names to apply the function to.
                                    If None, applies to all columns. Defaults to None.

    Returns:
        DataFrame: A new Spark DataFrame with the function applied to the specified columns.
    """
    if cols is None:
        cols = sdf.columns
    return sdf.withColumns(
        {col_name: col_func(col_name, **kwargs) for col_name in cols}
    )


def transforms(
    sdf: DataFrame, transformations: list[tuple[Callable, dict]]
) -> DataFrame:
    """
    Apply a series of transformations to a Spark DataFrame.

    Parameters:
        sdf (DataFrame): The input Spark DataFrame to be transformed.
        transformations (list): A list of transformations, where each transformation is a tuple
                        containing a function and a dictionary of keyword arguments to apply the function to.

    Returns:
        DataFrame: The transformed Spark DataFrame.

    Example:
        ```python
        >>> def add_one(sdf, col_name):
        ...     return sdf.withColumn(col_name, F.col(col_name) + 1)
        >>> df = spark.createDataFrame([(1,), (2,)], ["value"])
        >>> transformations = [(add_one, {"col_name": "value"})]
        >>> df.transform(transforms, transformations).show()
        +-----+
        |value|
        +-----+
        |    2|
        |    3|
        +-----+
        ```
    """
    for transformation_funcs, kwarg in transformations:
        assert callable(transformation_funcs), "transformation_funcs must be callable"
        assert isinstance(kwarg, dict), "kwarg must be a dictionary"
        sdf = sdf.transform(transformation_funcs, **kwarg)
    return sdf


def execute_transformation_blueprint(
    sdf: DataFrame, blueprint: dict[str, Column]
) -> DataFrame:
    """
    Executes a transformation blueprint on a Spark DataFrame.

    The transformation blueprint is a dictionary where keys are column names
    and values are the corresponding transformations to apply. The function
    applies each transformation in the order specified by the blueprint and
    returns the resulting DataFrame with the transformed columns.

    A transformation_blueprint is a dictionary that the
    key: new column name
    value: Column function

    Args:
        sdf (DataFrame): The input DataFrame to be transformed.
        blueprint (Dict[str, Column]): A dictionary of column names as keys and
            transformation functions as values.

    Returns:
        DataFrame: The resulting DataFrame with the transformed columns.

    Example:
        ```python
        >>> df = spark.createDataFrame([(1,), (2,)], ["value"])
        >>> blueprint = {"value_plus_1": F.col("value") + 1}
        >>> df.transform(execute_transformation_blueprint, blueprint).show()
        +------------+
        |value_plus_1|
        +------------+
        |           2|
        |           3|
        +------------+
        ```
    """
    return sdf.select(
        [
            column_processing.alias(new_column_name)
            for new_column_name, column_processing in blueprint.items()
        ]
    )
