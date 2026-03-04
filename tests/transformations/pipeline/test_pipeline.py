import functools
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pysparky.transformations.pipeline import pipeline as te_pipeline


def test_apply_cols(spark):
    data = [("1", "John", "Doe"), ("2", "Jane", "Smith")]
    df = spark.createDataFrame(data, ["id", "first_name", "last_name"])
    result_df = te_pipeline.apply_cols(df, F.upper, ["first_name", "last_name"])

    result = result_df.collect()
    expected_data = [("1", "JOHN", "DOE"), ("2", "JANE", "SMITH")]
    assert result == expected_data


def test_transforms(spark):
    upper_cols_partial = functools.partial(te_pipeline.apply_cols, col_func=F.upper)
    plus_one_transformation = lambda sdf: sdf.withColumn("id", F.col("id") + 1)

    pipeline = [
        (upper_cols_partial, {"cols": ["name"]}),
        (plus_one_transformation, {}),
    ]

    data = [(1, "John"), (2, "Jane")]
    df = spark.createDataFrame(data, ["id", "name"])
    result_df = te_pipeline.transforms(df, pipeline)

    result = result_df.collect()
    expected_data = [(2, "JOHN"), (3, "JANE")]
    assert expected_data == result


def test_execute_transformation_blueprint(spark):
    data = [("Alice", "Engineering"), ("Bob", "HR"), ("Charlie", "Finance")]
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)

    blueprint = {
        "name_upper": F.upper("name"),
        "department_lower": F.lower("department"),
    }
    result_df = te_pipeline.execute_transformation_blueprint(df, blueprint)

    expected_data = [("ALICE", "engineering"), ("BOB", "hr"), ("CHARLIE", "finance")]
    expected_schema = T.StructType(
        [
            T.StructField("name_upper", T.StringType(), True),
            T.StructField("department_lower", T.StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert result_df.collect() == expected_df.collect()

if __name__ == "__main__":
    pytest.main([__file__])
