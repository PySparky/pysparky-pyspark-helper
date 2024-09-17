import pytest

from pysparky import quality


@pytest.mark.parametrize(
    argnames=["data"],
    argvalues=[
        pytest.param(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 2, "b": 20}], id="1:1"
        ),
        pytest.param(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 20}], id="N:1"
        ),
    ],
)
def test_expect_any_to_one_pass(spark, data):
    @quality.expect_any_to_one("a", "b")
    def return_sdf():
        return spark.createDataFrame(data)

    return_sdf()  # If no AssertionError raised, test passes


def test_expect_any_to_one_fail(spark):
    @quality.expect_any_to_one("a", "b")
    def return_sdf():
        return spark.createDataFrame(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 2, "b": 30}]
        )

    with pytest.raises(AssertionError):
        return_sdf()


def test_expect_one_to_one_pass(spark):
    @quality.expect_one_to_one("a", "b")
    def return_sdf():
        return spark.createDataFrame(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 2, "b": 20}]
        )

    return_sdf()  # If no AssertionError raised, test passes


@pytest.mark.parametrize(
    argnames=["data"],
    argvalues=[
        pytest.param(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 20}], id="N:1"
        ),
        pytest.param(
            [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 2, "b": 30}], id="1:N"
        ),
    ],
)
def test_expect_one_to_one_fail(spark, data):
    @quality.expect_one_to_one("a", "b")
    def return_sdf():
        return spark.createDataFrame(data)

    with pytest.raises(AssertionError):
        return_sdf()


if __name__ == "__main__":
    pytest.main([__file__])
