from pyspark.sql import functions as F
import itertools


def create_map_from_dict(dict_):
    """
    Creates a PySpark map column from a given dictionary.

    This function takes a dictionary and converts it into a PySpark map column
    where each key-value pair in the dictionary is represented as a literal in the map.

    Parameters:
    dict_ (Dict[str, int]): A dictionary where keys are strings and values are integers.

    Returns:
    Column: A PySpark Column object representing the map created from the dictionary.

    Example:
    >>> dict_ = {"a": 1, "b": 2}
    >>> map_column = create_map_from_dict(dict_)
    """
    return F.create_map(list(map(F.lit, itertools.chain(*dict_.items()))))