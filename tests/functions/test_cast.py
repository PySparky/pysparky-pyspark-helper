import pytest
from pysparky import functions as F_


def test_cast_string_to_boolean(spark):
    data = [
        ("True", True),
        ("true", True),
        ("T", True),
        ("t", True),
        ("1", True),
        ("False", False),
        ("false", False),
        ("F", False),
        ("f", False),
        ("0", False),
        ("unknown", None),
        ("", None),
        (None, None),
    ]

    schema = T.StructType(
        [
            T.StructField("input", T.StringType(), True),
            T.StructField("expected", T.BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)
    result_df = df.withColumn("output", F_.cast_string_to_boolean(F.col("input")))
    result_with_column_name_df = df.withColumn(
        "output", F_.cast_string_to_boolean("input")
    )

    result = result_df.select("output").collect()
    result_with_column_name = result_with_column_name_df.select("output").collect()
    expected = df.select("expected").collect()

    assert result == expected
    assert result_with_column_name == expected
    
if __name__ == "__main__":
    pytest.main()
