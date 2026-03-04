from pyspark.sql import DataFrame


def get_distinct_value_from_df_columns(
    df: DataFrame, columns_names: list[str], display: bool = True
) -> dict[str, list]:
    """
    Get distinct values from specified DataFrame columns and optionally display their counts.

    Args:
        df (DataFrame): The input Spark DataFrame.
        columns_names (list[str]): List of column names to process.
        display (bool): Whether to display the counts of distinct values. Default is True.

    Returns:
        dict[str, list]: A dictionary where keys are column names and values are lists of distinct values.
    """
    myDict = {}
    for col in columns_names:
        data = df.select(col).distinct()
        myDict[col] = [row[col] for row in data.collect()]

        if display:
            if df.groupBy(col).count().count() < 20:
                df.groupBy(col).count().show()
    return myDict
