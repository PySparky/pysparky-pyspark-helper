from pyspark.sql import Column, Window
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

    Examples:
        ```python
        haversine_distance(F.lit(52.1552), F.lit(5.3876), F.lit(59.9111), F.lit(10.7503))
        923.8038067341608
        ```
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


@decorator.pyspark_column_or_name_enabler("columnOrName")
def cumsum(
    columnOrName: Column,
    partitionBy: list[Column] = [],
    is_normalized: bool = False,
    is_descending: bool = False,
    alias: str = "cumsum",
) -> Column:
    """
    Calculate the cumulative sum of a column, optionally partitioned by other columns.

    Args:
        columnOrName (Column): The column for which to calculate the cumulative sum.
        partitionBy (list[Column], optional): A list of columns to partition by. Defaults to an empty list.
        is_normalized (bool, optional): Whether to normalize the cumulative sum. Defaults to False.
        is_descending (bool, optional): Whether to order the cumulative sum in descending order. Defaults to False.
        alias (str, optional): Alias for the resulting column. Defaults to "cumsum".

    Returns:
        Column: A column representing the cumulative sum.

    Example:
        >>> df = spark.createDataFrame([(1, "A", 10), (2, "A", 20), (3, "B", 30)], ["id", "category", "value"])
        >>> result_df = df.select("id", "category", "value", cumsum(F.col("value"), partitionBy=[F.col("category")], is_descending=True))
        >>> result_df.display()
    """
    if is_normalized:
        total_sum = F.sum(columnOrName).over(Window.partitionBy(partitionBy))
    else:
        total_sum = F.lit(1)

    if is_descending:
        columnOrName_ordered = columnOrName.desc()
    else:
        columnOrName_ordered = columnOrName.asc()

    cumsum = F.sum(columnOrName).over(
        Window.partitionBy(partitionBy).orderBy(columnOrName_ordered)
    )

    return (cumsum / total_sum).alias(alias)
