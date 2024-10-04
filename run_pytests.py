import sys

import pytest

sys.path.append(".")

sys.dont_write_bytecode = True

args = ["--verbose", "-p", "no:cacheprovider"]
# args += ["-k", "test_dataframe_transform"] # uncomment for specific test

result = pytest.main(args)

print(f"{result=}")
assert result == pytest.ExitCode.OK, "Test run was not successful."
