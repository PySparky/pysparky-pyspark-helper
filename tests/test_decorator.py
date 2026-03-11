import pytest

# Now import the decorators
from pyspark.sql import DataFrame
from pysparky.decorator import extension_enabler, validate_schema


def test_validate_schema_success(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_schema(inputs=["user_id", "raw_revenue"], outputs=["net_revenue"])
    def calculate_net_revenue(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    result = calculate_net_revenue(df)
    assert "net_revenue" in result.columns
    assert result.collect()[0]["net_revenue"] == 80.0


def test_validate_schema_missing_input(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "other_col"])

    @validate_schema(inputs=["user_id", "raw_revenue"], outputs=["net_revenue"])
    def calculate_net_revenue(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    with pytest.raises(ValueError, match="Function calculate_net_revenue failed: Missing input columns \\['raw_revenue'\\]"):
        calculate_net_revenue(df)


def test_validate_schema_missing_output(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_schema(inputs=["user_id", "raw_revenue"], outputs=["net_revenue"])
    def faulty_calculate_net_revenue(df: DataFrame) -> DataFrame:
        # Fails to create the required output column
        return df.withColumn("wrong_name", df["raw_revenue"] * 0.8)

    with pytest.raises(ValueError, match="Function faulty_calculate_net_revenue failed: Expected output columns \\['net_revenue'\\] missing"):
        faulty_calculate_net_revenue(df)


def test_extension_enabler():
    class TestClass:
        def __init__(self):
            self.value = 0

    @extension_enabler(TestClass)
    def add_value(self, x):
        self.value += x
        return self

    # Test if the function is added to the class
    assert hasattr(TestClass, "add_value")

    # Test if the function works as expected
    obj = TestClass()
    obj.add_value(5)
    assert obj.value == 5

    # Test chaining
    obj.add_value(3).add_value(2)
    assert obj.value == 10


if __name__ == "__main__":
    pytest.main()
