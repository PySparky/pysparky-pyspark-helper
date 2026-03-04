import pytest

from pysparky.transformations.aggregations import aggregations as te_aggregations


def test_distinct_value_counts_map(spark):
    data = [("Alice",), ("Bob",), ("Alice",), ("Eve",), (None,)]
    sample_data = spark.createDataFrame(data, ["name"])
    result = te_aggregations.distinct_value_counts_map(sample_data, "name")
    expected_data = [({"Alice": 2, "Bob": 1, "Eve": 1, "NONE": 1},)]
    expected_df = spark.createDataFrame(expected_data, ["name_map"])

    result_list = result.collect()
    expected_list = expected_df.collect()

    assert result_list == expected_list

if __name__ == "__main__":
    pytest.main([__file__])
