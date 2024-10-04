import itertools

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def create_map_from_dict(dict_: dict[str, int]) -> Column:
    """
    Generates a PySpark map column from a provided dictionary.

    This function converts a dictionary into a PySpark map column, with each key-value pair represented as a literal in the map.

    Parameters:
        dict_ (Dict[str, int]): A dictionary with string keys and integer values.

    Returns:
        Column: A PySpark Column object representing the created map.

    Examples:
        >>> dict_ = {"a": 1, "b": 2}
        >>> map_column = create_map_from_dict(dict_)
    """

    return F.create_map(list(map(F.lit, itertools.chain(*dict_.items()))))


def join_dataframes_on_column(column_name: str, *dataframes: DataFrame) -> DataFrame:
    """
    Joins a list of DataFrames on a specified column using an outer join.

    Args:
        column_name (str): The column name to join on.
        *dataframes (DataFrame): A list of DataFrames to join.

    Returns:
        DataFrame: The resulting DataFrame after performing the outer joins.
    """
    if not dataframes:
        raise ValueError("At least one DataFrame must be provided")

    joined_df = dataframes[0].select(F.col(column_name))
    for sdf in dataframes:
        joined_df = joined_df.join(sdf, column_name, "outer").fillna(0)
    return joined_df


def union_dataframes(*dataframes: DataFrame) -> DataFrame:
    """
    Unions a list of DataFrames.

    Args:
        *dataframes (DataFrame): A list of DataFrames to union.

    Returns:
        DataFrame: The resulting DataFrame after performing the unions.
    """
    # TODO: Check on the schema, if not align, raise error

    if not dataframes:
        raise ValueError("At least one DataFrame must be provided")

    output_df = dataframes[0]
    for sdf in dataframes[1:]:
        output_df = output_df.union(sdf)
    return output_df
