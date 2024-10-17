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


def test_column_name_or_column_names_enabler_with_string():
    result = enabler.column_name_or_column_names_enabler("col1")
    expected = ["col1"]
    assert result == expected


def test_column_name_or_column_names_enabler_with_list():
    result = enabler.column_name_or_column_names_enabler(["col1", "col2"])
    expected = ["col1", "col2"]
    assert result == expected


def test_column_name_or_column_names_enabler_with_empty_list():
    result = enabler.column_name_or_column_names_enabler([])
    expected = []
    assert result == expected


if __name__ == "__main__":
    pytest.main()
