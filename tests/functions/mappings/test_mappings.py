import pytest
from pyspark.sql import functions as F

from pysparky.functions.mappings import mappings as F_mappings


def test_get_value_from_map(spark):
    dict_ = {1: "a", 2: "b", 3: "c"}
    input_sdf = spark.createDataFrame(data=[(1,), (2,), (3,)], schema=["key"])
    expected_sdf = spark.createDataFrame(
        data=[(1, "a"), (2, "b"), (3, "c")], schema=["key", "value"]
    )
    actual_sdf = input_sdf.withColumn(
        "value", F_mappings.get_value_from_map(column_or_name="key", dict_=dict_)
    )

    assert expected_sdf.collect() == actual_sdf.collect()

    actual2_sdf = input_sdf.withColumn(
        "value", F_mappings.get_value_from_map(column_or_name=F.col("key"), dict_=dict_)
    )
    assert expected_sdf.collect() == actual2_sdf.collect()


def test_when_mapping(spark):
    dict_ = {1: "a", 2: "b", 3: "c"}
    input_sdf = spark.createDataFrame(data=[(1,), (2,), (3,)], schema=["key"])
    expected_sdf = spark.createDataFrame(
        data=[(1, "a"), (2, "b"), (3, "c")], schema=["key", "value"]
    )
    actual_sdf = input_sdf.withColumn(
        "value", F_mappings.when_mapping(column_or_name="key", dict_=dict_)
    )

    assert expected_sdf.collect() == actual_sdf.collect()

    actual2_sdf = input_sdf.withColumn(
        "value", F_mappings.when_mapping(column_or_name=F.col("key"), dict_=dict_)
    )
    assert expected_sdf.collect() == actual2_sdf.collect()


if __name__ == "__main__":
    pytest.main([__file__])
