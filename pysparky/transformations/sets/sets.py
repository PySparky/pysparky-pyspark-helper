from pyspark.sql import DataFrame

def get_unique_values(df1: DataFrame, df2: DataFrame, column_name: str) -> DataFrame:
    """Unions two DataFrames and returns a DataFrame with unique values.

    Args:
        df1 (DataFrame): First DataFrame.
        df2 (DataFrame): Second DataFrame.
        column_name (str): The column name containing the values.

    Returns:
        DataFrame: A DataFrame with unique values.

    Example:
        ```python
        >>> df1 = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        >>> df2 = spark.createDataFrame([(3,), (4,), (5,)], ["value"])
        >>> unique_values = get_unique_values(df1, df2, "value")
        >>> unique_values.sort("value").show()
        +-----+
        |value|
        +-----+
        |    1|
        |    2|
        |    3|
        |    4|
        |    5|
        +-----+
        ```
    """
    # Union the DataFrames
    union_df = df1.select(column_name).union(df2.select(column_name))

    # Perform distinct to get unique values
    unique_values_df = union_df.distinct()

    return unique_values_df
