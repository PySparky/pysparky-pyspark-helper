import pytest

# Now import the decorators
from pyspark.sql import DataFrame
from pysparky.decorator import extension_enabler, validate_columns


def test_validate_columns_input_exact(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(input_cols=["user_id", "raw_revenue"])
    def func(df: DataFrame) -> DataFrame:
        return df

    # Success
    func(df)

    @validate_columns(input_cols=["user_id"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df

    # Failure
    with pytest.raises(ValueError, match="Exact input columns \\['user_id'\\] did not match actual \\['user_id', 'raw_revenue'\\]"):
        func_fail(df)


def test_validate_columns_required(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(required_cols=["user_id"])
    def func(df: DataFrame) -> DataFrame:
        return df

    # Success
    func(df)

    @validate_columns(required_cols=["missing_col"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df

    # Failure
    with pytest.raises(ValueError, match="Missing required input columns \\['missing_col'\\]"):
        func_fail(df)


def test_validate_columns_expected(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(expected_cols=["net_revenue"])
    def func(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Success
    func(df)

    @validate_columns(expected_cols=["net_revenue"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df

    # Failure
    with pytest.raises(ValueError, match="Missing expected output columns \\['net_revenue'\\]"):
        func_fail(df)


def test_validate_columns_output_exact(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(output_cols=["user_id", "raw_revenue", "net_revenue"])
    def func(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Success
    func(df)

    @validate_columns(output_cols=["user_id", "net_revenue"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Failure
    with pytest.raises(ValueError, match="Exact output columns \\['user_id', 'net_revenue'\\] did not match actual \\['user_id', 'raw_revenue', 'net_revenue'\\]"):
        func_fail(df)


def test_validate_columns_added(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(added_cols=["net_revenue"])
    def func(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Success
    func(df)

    @validate_columns(added_cols=["net_revenue", "extra"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Failure
    with pytest.raises(ValueError, match="Added columns \\['net_revenue'\\] did not exactly match expected \\['net_revenue', 'extra'\\]"):
        func_fail(df)


def test_validate_columns_dropped(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(dropped_cols=["raw_revenue"])
    def func(df: DataFrame) -> DataFrame:
        return df.drop("raw_revenue")

    # Success
    func(df)

    @validate_columns(dropped_cols=["raw_revenue"])
    def func_fail(df: DataFrame) -> DataFrame:
        return df

    # Failure
    with pytest.raises(ValueError, match="Dropped columns \\[\\] did not exactly match expected \\['raw_revenue'\\]"):
        func_fail(df)


def test_validate_columns_instance_method(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    class RevenueCalculator:
        @validate_columns(required_cols=["user_id", "raw_revenue"], expected_cols=["net_revenue"])
        def calculate(self, df: DataFrame) -> DataFrame:
            return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    calculator = RevenueCalculator()
    result = calculator.calculate(df)
    assert "net_revenue" in result.columns


def test_validate_columns_kwargs(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(required_cols=["user_id", "raw_revenue"], expected_cols=["net_revenue"])
    def calculate_net_revenue(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    result = calculate_net_revenue(df=df)
    assert "net_revenue" in result.columns


def test_validate_columns_no_df():
    @validate_columns(required_cols=["user_id"])
    def invalid_function(not_a_df):
        pass

    with pytest.raises(ValueError, match="Function invalid_function failed: No DataFrame argument found"):
        invalid_function("hello")


def test_validate_columns_invalid_return(spark):
    df = spark.createDataFrame([(1, 100.0)], ["user_id", "raw_revenue"])

    @validate_columns(required_cols=["user_id"])
    def invalid_return_function(df: DataFrame):
        return "not a dataframe"

    with pytest.raises(ValueError, match="Function invalid_return_function failed: Did not return a DataFrame"):
        invalid_return_function(df)


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
