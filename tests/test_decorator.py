import pytest
from pyspark.sql import Column
from pyspark.sql import functions as F

# Now import the decorators
from pysparky.decorator import extension_enabler


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
