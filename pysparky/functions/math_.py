from pyspark.sql import Column
from pyspark.sql import functions as F

from pysparky import decorator


@decorator.extension_enabler(Column)
@decorator.pyspark_column_or_name_enabler("lat1", "long1", "lat2", "long2")
def haversine_distance(
    lat1: Column,
    long1: Column,
    lat2: Column,
    long2: Column,
) -> Column:
    """
    Calculates the Haversine distance between two sets of latitude and longitude coordinates.

    Args:
        lat1 (Column): The column containing the latitude of the first coordinate.
        long1 (Column): The column containing the longitude of the first coordinate.
        lat2 (Column): The column containing the latitude of the second coordinate.
        long2 (Column): The column containing the longitude of the second coordinate.

    Returns:
        Column: The column containing the calculated Haversine distance.

    Example:
        # 923.8038067341608
        haversine_distance(F.lit(52.1552), F.lit(5.3876), F.lit(59.9111), F.lit(10.7503))
    """
    # Convert latitude and longitude from degrees to radians
    lat1_randians = F.radians(lat1)
    long1_randians = F.radians(long1)
    lat2_randians = F.radians(lat2)
    long2_randians = F.radians(long2)

    # Haversine formula
    dlat = lat2_randians - lat1_randians
    dlong = long2_randians - long1_randians
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(lat1_randians) * F.cos(lat2_randians) * F.sin(dlong / 2) ** 2
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    # Radius of the Earth (in kilometers)
    R = 6371.0

    # Calculate the distance
    distance = F.round(R * c, 4)

    return distance.alias("haversine_distance")
