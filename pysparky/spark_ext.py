from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from pysparky import decorator, enabler


@decorator.extension_enabler(SparkSession)
def column_function(spark: SparkSession, column_obj: Column) -> DataFrame:
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

    Example:
        ``` py
        from pyspark.sql import SparkSession, functions as F
        spark = SparkSession.builder.getOrCreate()
        ```
        # Simple column expression
        ``` py
        result = spark.column_function(F.lit("Hello, World!"))
        result.show()
        +-------------+
        |         col0|
        +-------------+
        |Hello, World!|
        +-------------+
        ```

        # Complex column expression
        ``` py
        import datetime
        complex_col = F.when(F.current_date() > F.lit(datetime.date(2023, 1, 1)), "Future")
        ...                .otherwise("Past")
        result = spark.column_function(complex_col)
        result.show()
        +------+
        |  col0|
        +------+
        |Future|
        +------+
        ```

        # Using with user-defined functions (UDFs)
        ``` py
        from pyspark.sql.types import IntegerType
        square_udf = F.udf(lambda x: x * x, IntegerType())
        result = spark.column_function(square_udf(F.lit(5)))
        result.show()
        +----+
        |col0|
        +----+
        |  25|
        +----+
        ```

    Notes:
        - This function is particularly useful for debugging or testing Column expressions
          without the need to create a full DataFrame.
        - The resulting DataFrame will always have a single column named 'col0' unless
          the input Column object has a specific alias.
        - Be cautious when using this with resource-intensive operations, as it still
          creates a distributed DataFrame operation.
    """
    return spark.range(1).select(column_obj)


@decorator.extension_enabler(SparkSession)
def convert_dict_to_dataframe(
    spark: SparkSession,
    dict_: dict[str, Any],
    column_names: list[str],
    explode: bool = False,
) -> DataFrame:
    """
    Transforms a dictionary with list values into a Spark DataFrame.

    Args:
        dict_ (dict): The dictionary to transform. Keys will become the first column, and values will become the second column.
        column_names (list[str]): A list containing the names of the columns.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with the dictionary keys and their corresponding exploded list values.

    Example:
        ``` py
        datadict_ = {
            "key1": 1,
            "key2": 2
        }
        column_names = ["keys", "values"]
        df = convert_dict_to_dataframe(datadict_, column_names)
        display(df)
        # key1,1
        # key2,2
        ```

    """

    # Assert that the input dictionary is not empty and column_names has exactly two elements
    assert isinstance(dict_, dict), "Input must be a dictionary"
    assert len(column_names) == 2, "Column names list must contain exactly two elements"

    output_sdf = spark.createDataFrame(dict_.items(), column_names)

    if explode:
        assert all(
            isinstance(val, list) for val in dict_.values()
        ), "All values in the dictionary must be lists"
        output_sdf = output_sdf.withColumn(column_names[1], F.explode(column_names[1]))

    return output_sdf


@decorator.extension_enabler(SparkSession)
# @decorator.column_name_or_column_names_enabler("column_names")
def convert_1d_list_to_dataframe(
    spark: SparkSession,
    list_: list[Any],
    column_names: str | list[str],
    axis: str = "column",
) -> DataFrame:
    """
    Converts a 1-dimensional list into a PySpark DataFrame.

    This function takes a 1-dimensional list and converts it into a PySpark DataFrame
    with the specified column names. The list can be converted into a DataFrame with
    either a single column or a single row, based on the specified axis.

    Parameters:
        spark (SparkSession): The Spark session to use for creating the DataFrame.
        list_ (list): The 1-dimensional list to convert.
        column_names (str or list of str): The name(s) of the column(s) for the DataFrame.
        axis (str): Specifies whether to convert the list into a single column or a single row.
                    Acceptable values are "column" (default) and "row".

    Returns:
        DataFrame: A PySpark DataFrame created from the 1-dimensional list.

    Raises:
        AttributeError: If the axis parameter is not "column" or "row".

    Example:
        ``` py
        >>> spark = SparkSession.builder.appName("example").getOrCreate()
        >>> list_ = [1, 2, 3, 4]
        >>> column_names = ["numbers"]
        >>> df = convert_1d_list_to_dataframe(spark, list_, column_names, axis="column")
        >>> df.show()
        +-------+
        |numbers|
        +-------+
        |      1|
        |      2|
        |      3|
        |      4|
        +-------+
        >>> column_names = ["ID1", "ID2", "ID3", "ID4"]
        >>> df = convert_1d_list_to_dataframe(spark, list_, column_names, axis="row")
        >>> df.show()
        +---+---+---+---+
        |ID1|ID2|ID3|ID4|
        +---+---+---+---+
        |  1|  2|  3|  4|
        +---+---+---+---+
        ```
    """
    column_names = enabler.ensure_list(column_names)

    if axis not in ["column", "row"]:
        raise AttributeError(
            f"Invalid axis value: {axis}. Acceptable values are 'column' or 'row'."
        )

    if axis == "column":
        tuple_list = ((x,) for x in list_)  # type: ignore
    elif axis == "row":
        tuple_list = (tuple(list_),)  # type: ignore

    output_sdf = spark.createDataFrame(tuple_list, schema=column_names)

    return output_sdf


@decorator.extension_enabler(SparkSession)
def createDataFrame_from_dict(spark: SparkSession, dict_: dict) -> DataFrame:
    """
    Creates a Spark DataFrame from a dictionary in a pandas-like style.

    Args:
        spark: The SparkSession object.
        dict_ (dict): The dictionary to convert, where keys are column names and values are lists of column data.

    Returns:
        DataFrame: The resulting Spark DataFrame.
    """
    data = list(zip(*dict_.values()))
    label = list(dict_.keys())
    return spark.createDataFrame(data, label)


def check_table_exists(
    spark: SparkSession, catalog: str, database: str, table_name: str
) -> bool:
    """
    Checks if a specific table exists in the given catalog.

    Args:
        spark (SparkSession): The Spark session.
        catalog (str): The catalog name.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    # Retrieve the list of tables in the specified catalog
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()

    # Check if the specific table exists
    table_exists = any(table.tableName == table_name for table in tables)

    return table_exists
