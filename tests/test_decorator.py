import pytest
from pyspark.sql import Column

from .spark import spark

# Now import the decorators
from pysparky.decorator import pyspark_column_or_name_enabler, extension_enabler

# # Test pyspark_column_or_name_enabler
def test_pyspark_column_or_name_enabler():
    @pyspark_column_or_name_enabler("col1", "col2")
    def test_function(col1, col2, col3):
        return col1, col2, col3

    # Test with string input
    result = test_function("name1", "name2", "name3")
    assert isinstance(result[0], Column)
    assert isinstance(result[1], Column)
    assert isinstance(result[2], str)

    # Test with Column input
    col_input = Column("col_name")
    result = test_function(col_input, "name2", col_input)
    assert result[0] is col_input
    assert isinstance(result[1], Column)
    assert result[2] is col_input

    # Test with keyword arguments
    result = test_function(col1="name1", col2=Column("col2"), col3="name3")
    assert isinstance(result[0], Column)
    assert isinstance(result[1], Column)
    assert isinstance(result[2], str)

# Test extension_enabler
def test_extension_enabler():
    class TestClass:
        def __init__(self):
            self.value = 0

    @extension_enabler(TestClass)
    def add_value(self, x):
        self.value += x
        return self

    # Test if the function is added to the class
    assert hasattr(TestClass, "add_value")

    # Test if the function works as expected
    obj = TestClass()
    obj.add_value(5)
    assert obj.value == 5

    # Test chaining
    obj.add_value(3).add_value(2)
    assert obj.value == 10

if __name__ == "__main__":
    pytest.main()