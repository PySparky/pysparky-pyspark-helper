import pytest
from pyspark.sql import types as T

from pysparky import schema_ext as se


def test_filter_columns_by_datatype():
    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("price", T.DecimalType(9, 2), True),
            T.StructField("timestamp", T.TimestampType(), True),
        ]
    )

    # Test for StringType
    string_columns = se.filter_columns_by_datatype(schema, T.StringType())
    expected_string_columns = ["id", "name"]
    assert string_columns.names == expected_string_columns

    # Test for IntegerType
    integer_columns = se.filter_columns_by_datatype(schema, T.IntegerType())
    expected_integer_columns = ["age"]
    assert integer_columns.names == expected_integer_columns

    # Test for DecimalType
    decimal_columns = se.filter_columns_by_datatype(schema, T.DecimalType(9, 2))
    expected_decimal_columns = ["price"]
    assert decimal_columns.names == expected_decimal_columns

    # Test for TimestampType
    timestamp_columns = se.filter_columns_by_datatype(schema, T.TimestampType())
    expected_timestamp_columns = ["timestamp"]
    assert timestamp_columns.names == expected_timestamp_columns


if __name__ == "__main__":
    pytest.main([__file__])
