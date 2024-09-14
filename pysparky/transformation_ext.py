import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F

from pysparky import decorator


# the fuction is having some error, need changes
@decorator.extension_enabler(pyspark.sql.DataFrame)
def get_latest_record_from_column(
    sdf: pyspark.sql.DataFrame,
    window_partition_column_name: str,
    window_order_by_column_names: str | list,
    window_function: pyspark.sql.Column = F.row_number,
) -> pyspark.sql.DataFrame:
    """
    Returns the latest record from a DataFrame based on a specified column. You have to specify your own desc asc

    Parameters:
        sdf (DataFrame): The input DataFrame.
        window_partition_column_name (str): The column used for partitioning the DataFrame.
        window_order_by_column_names (str | list): The column used for ordering the DataFrame.
        is_desc (bool, optional): The order in which the DataFrame is sorted. Can be either True(desc) or False(asc).
            Defaults to True.
        window_function (Column, optional): The window function to be used for ranking the records.
            Defaults to F.row_number.


    Returns:
        DataFrame: The resulting DataFrame containing the latest record for each partition.
    """
    if not isinstance(window_order_by_column_names, list):
        window_order_by_column_names = list(window_order_by_column_names)

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
