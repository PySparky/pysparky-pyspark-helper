import pytest
from pyspark.sql import functions as F

from pysparky import functions_math


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
            functions_math.haversine_distance(
                F.lit(52.1552), F.lit(5.3876), F.lit(59.9111), F.lit(10.7503)
            )
        )
        .collect()[0][0]
    )
    assert distance_km_round4 == target_value


if __name__ == "__main__":
    pytest.main([__file__])
