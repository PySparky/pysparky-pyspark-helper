import pytest
from pyspark.sql import functions as F

from pysparky import functions as F_
from pysparky.functions.math_ import haversine_distance


def test_haversine_distance(spark):
    import math

    def haversine(lat1, lon1, lat2, lon2):
        # Convert latitude and longitude from degrees to radians
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        # Radius of the Earth (in kilometers)
        R = 6371.0

        # Calculate the distance
        distance = R * c

        return distance

    # Example usage (Ans:923.8038)
    distance_km = haversine(52.1552, 5.3876, 59.9111, 10.7503)
    distance_km_round4 = round(distance_km, 4)

    target_value = (
        spark.range(1)
        .select(
            haversine_distance(
                F.lit(52.1552), F.lit(5.3876), F.lit(59.9111), F.lit(10.7503)
            )
        )
        .collect()[0][0]
    )
    assert distance_km_round4 == target_value

    target_value2 = (
        spark.range(1)
        .select(
            F_.haversine_distance(
                F.lit(52.1552), F.lit(5.3876), F.lit(59.9111), F.lit(10.7503)
            )
        )
        .collect()[0][0]
    )
    assert distance_km_round4 == target_value2


def test_cumsum(spark):
    data = [
        (1, "A", 10),
        (2, "A", 20),
        (3, "B", 30),
        (4, "B", 40),
        (5, "A", 50),
        (6, "B", 60),
    ]
    df = spark.createDataFrame(data, ["id", "category", "value"])

    result_df = df.select(
        "id",
        "category",
        "value",
        F_.cumsum(F.col("value"), partition_by=[F.col("category")]),
    )

    expected_data = [
        (1, "A", 10, 10),
        (2, "A", 20, 30),
        (5, "A", 50, 80),
        (3, "B", 30, 30),
        (4, "B", 40, 70),
        (6, "B", 60, 130),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["id", "category", "value", "cumsum"]
    )

    assert result_df.collect() == expected_df.collect()


def test_desc_cumsum(spark):
    data = [
        (1, "A", 10),
        (2, "A", 20),
        (3, "B", 30),
        (4, "B", 40),
        (5, "A", 50),
        (6, "B", 60),
    ]
    df = spark.createDataFrame(data, ["id", "category", "value"])

    result_df = df.select(
        "id",
        "category",
        "value",
        F_.cumsum("value", is_descending=True),
    ).sort(F.col("id").asc())

    expected_data = [
        (1, "A", 10, 210),
        (2, "A", 20, 200),
        (3, "B", 30, 180),
        (4, "B", 40, 150),
        (5, "A", 50, 110),
        (6, "B", 60, 60),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["id", "category", "value", "cumsum"]
    )

    assert result_df.collect() == expected_df.collect()


def test_normalized_cumsum(spark):
    data = [
        (1, "A", 10),
        (2, "A", 20),
        (3, "B", 30),
        (4, "B", 40),
        (5, "A", 50),
        (6, "B", 60),
    ]
    df = spark.createDataFrame(data, ["id", "category", "value"])

    result_df = df.select(
        "id",
        "category",
        "value",
        F_.cumsum(F.col("value"), partition_by=[F.col("category")], is_normalized=True),
    )

    expected_data = [
        (1, "A", 10, 10 / 80),
        (2, "A", 20, 30 / 80),
        (5, "A", 50, 80 / 80),
        (3, "B", 30, 30 / 130),
        (4, "B", 40, 70 / 130),
        (6, "B", 60, 130 / 130),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["id", "category", "value", "cumsum"]
    )

    assert result_df.collect() == expected_df.collect()


def test_sumif(spark):
    data = [
        (1, "A", 10),
        (2, "A", 20),
        (3, "B", 30),
        (4, "B", 40),
        (5, "A", 50),
        (6, "B", 60),
    ]
    df = spark.createDataFrame(data, ["id", "category", "value"])

    # 1. Sum count where category is 'A' (value defaults to 1)
    result_count = df.select(
        F_.sumif(F.col("category") == "A").alias("count_a")
    ).collect()[0]["count_a"]
    assert result_count == 3

    # 2. Sum value where category is 'A'
    result_sum = df.select(
        F_.sumif(F.col("category") == "A", value=F.col("value")).alias("sum_a")
    ).collect()[0]["sum_a"]
    assert result_sum == 80

    # 3. Sum value where category is 'A', otherwise -100
    # Condition True (A): 10 + 20 + 50 = 80
    # Condition False (B): -100 + -100 + -100 = -300
    # Total = -220
    result_sum_otherwise = df.select(
        F_.sumif(
            F.col("category") == "A", value=F.col("value"), otherwise_value=-100
        ).alias("sum_a_otherwise")
    ).collect()[0]["sum_a_otherwise"]
    assert result_sum_otherwise == -220


if __name__ == "__main__":
    pytest.main([__file__])
