import pytest
from pyspark.sql import functions as F

import pysparky.transformations as te


def test_get_latest_record_from_column(spark):
    input_sdf = spark.createDataFrame(
        [
            (1, "Apple", "old"),
            (2, "Apple", "new"),
            (1, "Orange", "old"),
            (2, "Orange", "old"),
            (3, "Orange", "new"),
        ],
        ["ts", "product", "property"],
    )

    output_sdf = input_sdf.transform(
        te.get_latest_record_from_column,
        window_partition_column_name="product",
        window_order_by_column_names=F.col("ts").desc(),
        window_function=F.row_number,
    )

    target_sdf = spark.createDataFrame(
        [
            (2, "Apple", "new"),
            (3, "Orange", "new"),
        ],
        ["ts", "product", "property"],
    )

    # it will raise error with assertDataFrameEqual if there is an error
    assert output_sdf.collect() == target_sdf.collect()


@pytest.fixture
def sample_data(spark):
    data = [
        (1, "A"),
        (2, "B"),
        (3, "C"),
        (1, "A"),
        (4, "D"),
        (2, "B"),
    ]
    return spark.createDataFrame(data, ["id", "value"])


def test_quarantine_duplicate_record(spark, sample_data):
    unique_records, duplicate_records = te.quarantine_duplicate_record(
        sample_data, "id"
    )

    unique_expected = [(3, "C"), (4, "D")]
    duplicate_expected = [(1, "A"), (1, "A"), (2, "B"), (2, "B")]

    assert (
        unique_records.collect()
        == spark.createDataFrame(unique_expected, ["id", "value"]).collect()
    )
    assert (
        duplicate_records.collect()
        == spark.createDataFrame(duplicate_expected, ["id", "value"]).collect()
    )


def test_get_only_duplicate_record(spark, sample_data):
    duplicate_records = te.get_only_duplicate_record(sample_data, "id")

    duplicate_expected = [(1, "A"), (1, "A"), (2, "B"), (2, "B")]

    assert (
        duplicate_records.collect()
        == spark.createDataFrame(duplicate_expected, ["id", "value"]).collect()
    )


def test_get_only_unique_record(spark, sample_data):
    unique_records = te.get_only_unique_record(sample_data, "id")

    unique_expected = [(3, "C"), (4, "D")]

    assert (
        unique_records.collect()
        == spark.createDataFrame(unique_expected, ["id", "value"]).collect()
    )


if __name__ == "__main__":
    pytest.main([__file__])
