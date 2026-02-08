from pyspark.sql import types as T

from pysparky import decorator


@decorator.extension_enabler(T.StructType)
def filter_columns_by_datatype(
    struct_type: T.StructType, data_type: T.DataType
) -> T.StructType:
    """
    Filters and returns a StructType of StructField names from a given StructType schema
    that match the specified data type.

    Args:
        struct_type (T.StructType): The schema of the DataFrame.
        data_type (T.DataType): The data type to filter by.

    Returns:
        T.StructType: A StructType of StructField names that match the specified data type.

    Example:
        ```python
        >>> schema = T.StructType([
        ...     T.StructField("id", T.IntegerType(), True),
        ...     T.StructField("name", T.StringType(), True),
        ...     T.StructField("age", T.IntegerType(), True)
        ... ])
        >>> filtered_schema = filter_columns_by_datatype(schema, T.IntegerType())
        >>> print(filtered_schema)
        StructType([StructField('id', IntegerType(), True), StructField('age', IntegerType(), True)])
        ```
    """
    return T.StructType([field for field in struct_type if field.dataType == data_type])
