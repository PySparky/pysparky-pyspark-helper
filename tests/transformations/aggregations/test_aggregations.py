import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pysparky.transformations.aggregations import aggregations as te_aggregations


def test_agg_apply(spark):
    data = [("A", 10), ("B", 20), ("A", 30)]
    df = spark.createDataFrame(data, ["category", "value"])

    agg_exprs = {
        "total_value": F.sum("value"),
        "max_value": F.max("value"),
        "count": F.count("*"),
    }

    result_df = te_aggregations.agg_apply(df, agg_exprs)
    result = result_df.collect()[0]

    assert result["total_value"] == 60
    assert result["max_value"] == 30
    assert result["count"] == 3


def test_order_and_aggregate_events(spark):
    data = [
        ("A", "val1", 2),
        ("A", "val2", 1),
        ("A", "val3", 3),
        ("B", "val4", 1),
        ("C", "val5", 5),
        ("C", "val6", 2),
    ]
    df = spark.createDataFrame(data, ["id", "value", "record_no"])

    result_df = te_aggregations.order_and_aggregate_events(
        df, "id", "value", "record_no", "values", "record_nos"
    )

    result = result_df.collect()
    result_sorted = sorted(result, key=lambda x: x["id"])

    assert result_sorted[0]["id"] == "A"
    assert result_sorted[0]["values"] == ["val2", "val1", "val3"]
    assert result_sorted[0]["record_nos"] == [1, 2, 3]

    assert result_sorted[1]["id"] == "B"
    assert result_sorted[1]["values"] == ["val4"]
    assert result_sorted[1]["record_nos"] == [1]

    assert result_sorted[2]["id"] == "C"
    assert result_sorted[2]["values"] == ["val6", "val5"]
    assert result_sorted[2]["record_nos"] == [2, 5]


def test_order_and_aggregate_events_desc(spark):
    data = [
        ("A", "val1", 2),
        ("A", "val2", 1),
        ("A", "val3", 3),
        ("B", "val4", 1),
        ("C", "val5", 5),
        ("C", "val6", 2),
    ]
    df = spark.createDataFrame(data, ["id", "value", "record_no"])

    result_df = te_aggregations.order_and_aggregate_events(
        df, "id", "value", "record_no", "values", "record_nos", sort_asc=False
    )

    result = result_df.collect()
    result_sorted = sorted(result, key=lambda x: x["id"])

    assert result_sorted[0]["id"] == "A"
    assert result_sorted[0]["values"] == ["val3", "val1", "val2"]
    assert result_sorted[0]["record_nos"] == [3, 2, 1]

    assert result_sorted[1]["id"] == "B"
    assert result_sorted[1]["values"] == ["val4"]
    assert result_sorted[1]["record_nos"] == [1]

    assert result_sorted[2]["id"] == "C"
    assert result_sorted[2]["values"] == ["val5", "val6"]
    assert result_sorted[2]["record_nos"] == [5, 2]
