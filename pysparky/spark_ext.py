from typing import Any

import pyspark
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(pyspark.sql.SparkSession)
def column_function(spark, column_obj: Column):
    """
    Evaluates a Column expression in the context of a single-row DataFrame.

    This function creates a DataFrame with a single row and applies the given Column
    expression to it. This is particularly useful for testing Column expressions,
    evaluating complex transformations, or creating sample data based on Column operations.

    Args:
        spark (pyspark.sql.SparkSession): The SparkSession object.
        column_obj (pyspark.sql.Column): The Column object or expression to evaluate.

    Returns:
        pyspark.sql.DataFrame: A single-row DataFrame containing the result of the Column expression.

    Examples:
        >>> from pyspark.sql import SparkSession, functions as F
        >>> spark = SparkSession.builder.getOrCreate()

        # Simple column expression
        >>> result = spark.column_function(F.lit("Hello, World!"))
        >>> result.show()
        +-------------+
        |         col0|
        +-------------+
        |Hello, World!|
        +-------------+

        # Complex column expression
        >>> import datetime
        >>> complex_col = F.when(F.current_date() > F.lit(datetime.date(2023, 1, 1)), "Future")
        ...                .otherwise("Past")
        >>> result = spark.column_function(complex_col)
        >>> result.show()
        +------+
        |  col0|
        +------+
        |Future|
        +------+

        # Using with user-defined functions (UDFs)
        >>> from pyspark.sql.types import IntegerType
        >>> square_udf = F.udf(lambda x: x * x, IntegerType())
        >>> result = spark.column_function(square_udf(F.lit(5)))
        >>> result.show()
        +----+
        |col0|
        +----+
        |  25|
        +----+

    Notes:
        - This function is particularly useful for debugging or testing Column expressions
          without the need to create a full DataFrame.
        - The resulting DataFrame will always have a single column named 'col0' unless
          the input Column object has a specific alias.
        - Be cautious when using this with resource-intensive operations, as it still
          creates a distributed DataFrame operation.
    """
    return spark.range(1).select(column_obj)


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
        output_sdf = output_sdf.withColumn(column_names[1], F.explode(column_names[1]))

    return output_sdf
