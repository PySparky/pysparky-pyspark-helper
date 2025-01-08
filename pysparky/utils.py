import itertools
from functools import reduce

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


def join_dataframes_on_column(
    column_name: str, *dataframes: DataFrame, how: str = "outer"
) -> DataFrame:
    """
    Joins a list of DataFrames on a specified column using an outer join.

    Args:
        column_name (str): The column name to join on.
        *dataframes (DataFrame): A list of DataFrames to join.
        how (str): The type of join to perform, passthrough to pyspark join (default is "outer").

    Returns:
        DataFrame: The resulting DataFrame after performing the outer joins.
    """

    if not dataframes:
        raise ValueError("At least one DataFrame must be provided")

    # Check if all DataFrames have the specified column
    if not all(column_name in df.columns for df in dataframes):
        raise ValueError(f"Column '{column_name}' not found in all DataFrames")

    # Use reduce to perform the outer join on all DataFrames
    joined_df = reduce(
        lambda df1, df2: df1.join(df2, on=column_name, how=how), dataframes
    )
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

    return reduce(lambda df1, df2: df1.union(df2), dataframes)


def split_dataframe_by_column(
    sdf: DataFrame, split_column: str
) -> dict[str, DataFrame]:
    """
    Splits a DataFrame into multiple DataFrames based on distinct values in a specified column.

    Parameters:
        sdf (DataFrame): The input Spark DataFrame.
        column_name (str): The column name to split the DataFrame by.

    Returns:
        dict[str, DataFrame]: A dictionary where keys are distinct column values and values are DataFrames.
    """
    # Get distinct values from the specified column
    unique_values = [
        row[split_column] for row in sdf.select(split_column).distinct().collect()
    ]

    # Create a dictionary to hold the filtered DataFrames
    filtered_dfs = {
        value: sdf.filter(F.col(split_column) == value) for value in unique_values
    }

    return filtered_dfs
