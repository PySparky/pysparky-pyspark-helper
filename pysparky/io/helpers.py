"""
Non PySparky specific helper functions for IO operations.
"""
import re


def extract_key_value_from_path(file_path: str) -> dict[str, str]:
    """
    Extracts key-value pairs from a file path using regex.

    Args:
        file_path (str): The file path from which to extract `key=value` pairs.
            PySpark write csv partitioned by will have the key value pairs in the path.

    Returns:
        dict[str, str]: A dictionary containing the extracted key-value pairs.

    Example:
        >>> extract_key_value_from_path("path/to/file/key1=value1/key2=value2/abc.csv")
        {'key1': 'value1', 'key2': 'value2'}
    """
    key_value_pairs = {}
    pattern = r"(?:^|/)([^/]+?)=([^/]*)"  # Match key=value pairs

    key_value_pairs = {
        match.group(1): match.group(2) for match in re.finditer(pattern, file_path)
    }

    return key_value_pairs
