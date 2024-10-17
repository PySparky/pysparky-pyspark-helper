import pytest
from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import enabler


def test_ensure_column_with_strings(spark):
    col1, col2, col3 = enabler.ensure_column("col1", "col2", "col3")
    assert isinstance(col1, Column)
    assert isinstance(col2, Column)
    assert isinstance(col3, Column)


def test_ensure_column_with_columns(spark):
    col1, col2 = enabler.ensure_column(F.col("col1"), F.col("col2"))
    assert isinstance(col1, Column)
    assert isinstance(col2, Column)


def test_ensure_column_with_1(spark):
    (col1,) = enabler.ensure_column("col1")
    assert isinstance(col1, Column)


def test_ensure_list_with_string():
    result = enabler.ensure_list("col1")
    expected = ["col1"]
    assert result == expected


def test_ensure_list_with_list():
    result = enabler.ensure_list(["col1", "col2"])
    expected = ["col1", "col2"]
    assert result == expected


def test_ensure_list_with_empty_list():
    result = enabler.ensure_list([])
    expected = []
    assert result == expected


if __name__ == "__main__":
    pytest.main()
