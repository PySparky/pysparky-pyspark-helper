import pytest

from pysparky import debug


# Pytest
def test_get_distinct_value_from_df_columns(spark):
    data = [("Alice", 1), ("Bob", 2), ("Alice", 3), ("Eve", 4), (None, 5)]
    sdf = spark.createDataFrame(data, ["name", "id"])

    result = debug.get_distinct_value_from_df_columns(
        sdf, ["name", "id"], display=False
    )

    assert set(result["name"]) == {"Alice", "Bob", "Eve", None}
    assert set(result["id"]) == {1, 2, 3, 4, 5}
