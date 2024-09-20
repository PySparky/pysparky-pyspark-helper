import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(pyspark.sql.DataFrame)
def get_latest_record_from_column(
    sdf: pyspark.sql.DataFrame,
    window_partition_column_name: str,
    window_order_by_column_names: str | list,
    window_function: pyspark.sql.Column = F.row_number,
) -> pyspark.sql.DataFrame:
    """
    Fetches the most recent record from a DataFrame based on a specified column, allowing for custom sorting order.

    Parameters:
        sdf (DataFrame): The DataFrame to process.
        window_partition_column_name (str): The column used to partition the DataFrame.
        window_order_by_column_names (str | list): The column(s) used to sort the DataFrame.
        is_desc (bool, optional): Determines the sorting order. Set to True for descending (default) or False for ascending.
        window_function (Column, optional): The window function for ranking records. Defaults to F.row_number.

    Returns:
        DataFrame: A DataFrame with the most recent record for each partition.
    """

    if not isinstance(window_order_by_column_names, list):
        window_order_by_column_names = [window_order_by_column_names]

    return (
        sdf.withColumn(
            "temp",
            window_function().over(
                Window.partitionBy(window_partition_column_name).orderBy(
                    *window_order_by_column_names
                )
            ),
        )
        .filter(F.col("temp") == 1)
        .drop("temp")
    )
