import pytest

from pysparky.io import helpers as io_helpers


def test_extract_key_value_from_path():
    """Tests the extract_key_value_from_path function."""

    test_cases = [
        ("/path/key1=value1/key2=value2/abc.csv", {"key1": "value1", "key2": "value2"}),
        (
            "/path/key1=value1/key2=value2/more=values/abc.csv",
            {"key1": "value1", "key2": "value2", "more": "values"},
        ),
        (
            "/path/key1=value1=extra/key2=value2/abc.csv",
            {"key1": "value1=extra", "key2": "value2"},
        ),
        ("/path/key1=value1/abc.csv", {"key1": "value1"}),
        ("/path/key1=/abc.csv", {"key1": ""}),
        ("/path/key1/abc.csv", {}),  # No equals sign
        (
            "key1=value1/key2=value2/abc.csv",
            {"key1": "value1", "key2": "value2"},
        ),  # No leading slash
        ("", {}),  # Empty string
        ("/", {}),  # Only a slash
        ("//key1=value1", {"key1": "value1"}),  # Double slash
    ]

    for file_path, expected_output in test_cases:
        actual_output = io_helpers.extract_key_value_from_path(file_path)
        assert (
            actual_output == expected_output
        ), f"Input: {file_path}, Expected: {expected_output}, Actual: {actual_output}"


if __name__ == "__main__":
    pytest.main([__file__])
