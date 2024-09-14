from typing import Any

import pyspark
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(pyspark.sql.SparkSession)
def convert_dict_to_dataframe(
    spark, _dict: dict[str, Any], column_names: list[str], explode: bool = False
) -> pyspark.sql.DataFrame:
    """
    Converts a dictionary with list values into a Spark DataFrame.

    Args:
        _dict (dict): The dictionary to convert. Keys will be the first column, values will be the second column.
        column_names (list[str]): List containing the names of the columns.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with the dictionary keys and exploded list values.

    Example:
        data_dict = {
            "key1": 1,
            "key2": 2
        }
        column_names = ["keys", "values"]
        df = convert_dict_to_dataframe(data_dict, column_names)
        display(df)
        # key1,1
        # key2,2
    """

    # Assert that the input dictionary is not empty and column_names has exactly two elements
    assert isinstance(_dict, dict), "Input must be a dictionary"
    assert len(column_names) == 2, "Column names list must contain exactly two elements"

    output_sdf = spark.createDataFrame(_dict.items(), column_names)

    if explode:
        assert all(
            isinstance(val, list) for val in _dict.values()
        ), "All values in the dictionary must be lists"
        return output_sdf.withColumn(column_names[1], F.explode(column_names[1]))
    else:
        return output_sdf
