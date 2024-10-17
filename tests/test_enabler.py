import pytest
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import enabler


def test_column_or_name_enabler_with_strings(spark):
    col1, col2, col3 = enabler.column_or_name_enabler("col1", "col2", "col3")
    assert isinstance(col1, Column)
    assert isinstance(col2, Column)
    assert isinstance(col3, Column)


def test_column_or_name_enabler_with_columns(spark):
    col1, col2 = enabler.column_or_name_enabler(F.col("col1"), F.col("col2"))
    assert isinstance(col1, Column)
    assert isinstance(col2, Column)


def test_column_or_name_enabler_with_1(spark):
    (col1,) = enabler.column_or_name_enabler("col1")
    assert isinstance(col1, Column)


if __name__ == "__main__":
    pytest.main()
