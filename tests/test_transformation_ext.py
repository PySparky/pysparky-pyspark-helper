import pytest
from pyspark.sql import functions as F

from pysparky import transformation_ext as te


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


if __name__ == "__main__":
    pytest.main([__file__])
