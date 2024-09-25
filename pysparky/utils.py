import itertools

from pyspark.sql import functions as F
from pyspark.sql.column import Column


def create_map_from_dict(dict_: dict[str, int]) -> Column:
    """
    Generates a PySpark map column from a provided dictionary.

    This function converts a dictionary into a PySpark map column, with each key-value pair represented as a literal in the map.

    Parameters:
        dict_ (Dict[str, int]): A dictionary with string keys and integer values.

    Returns:
        Column: A PySpark Column object representing the created map.

    Examples:
        >>> dict_ = {"a": 1, "b": 2}
        >>> map_column = create_map_from_dict(dict_)
    """

    return F.create_map(list(map(F.lit, itertools.chain(*dict_.items()))))
