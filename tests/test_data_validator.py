import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from pysparky.data_validator import DataValidator, ValidationRule


def test_validation_rule(spark):
    # Create a sample DataFrame
    data = [(1, "valid"), (2, "invalid"), (3, "valid")]
    sdf = spark.createDataFrame(data, ["id", "status"])

    # Define a validation rule
    rule = ValidationRule(
        name="status_check", conditions=[F.col("status") == "valid", F.col("id") == 3]
    )
    result = sdf.filter(rule.combined_condition).collect()
    expect = [(3, "valid")]
    # Check combined condition
    assert result == expect


def test_data_validator(spark):
    # Create a sample DataFrame
    data = [(1, "valid"), (2, "invalid"), (3, "valid")]
    sdf = spark.createDataFrame(data, ["id", "status"])

    # Define validation rules
    rules_dict = {"status_check": [F.col("status") == "valid"]}
    validator = DataValidator.from_dict(rules_dict)

    # Check query map
    query_map = validator.query_map
    result_data = sdf.withColumns(query_map).collect()
    expected_data = [(1, "valid", True), (2, "invalid", False), (3, "valid", True)]
    assert result_data == expected_data

    # Apply conditions
    sdf_with_conditions = validator.apply_conditions(sdf)
    assert "status_check" in sdf_with_conditions.columns

    # Filter invalid rows
    invalid_sdf = validator.filter_invalid(sdf)
    invalid_count = invalid_sdf.filter(F.col("status_check") == False).count()
    assert invalid_count == 1

    # Filter valid rows
    valid_sdf = validator.filter_valid(sdf)
    valid_count = valid_sdf.filter(F.col("status_check") == True).count()
    assert valid_count == 2


if __name__ == "__main__":
    pytest.main([__file__])
