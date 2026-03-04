# pysparky Repository Guidelines

Welcome to the `pysparky` project! This repository contains a collection of utility functions, transformations, and I/O handlers designed to simplify PySpark coding.

## Coding Standards
1. **No Decorators for Chaining:** We have deprecated and removed `@decorator.extension_enabler`. Do NOT use it or try to chain methods directly onto PySpark `Column` or `DataFrame` instances. Use standard function calls instead (e.g., `func(df)` rather than `df.func()`).
2. **Type Hints:** All function definitions MUST include complete Python type hints (e.g., `def my_func(col: Column) -> Column:`).
3. **Docstrings:** Every function MUST have a comprehensive docstring that explains its purpose, arguments, and return types. Importantly, it MUST include an `Example:` section with `>>>` style doctests demonstrating how to use the function.
4. **Testing:** All new features must be accompanied by unit tests. Run tests via `python -m pytest tests/` and ensure there are no regressions.

## Repository Organization
- `pysparky/core/`: Contains core PySpark enablers and type extensions.
- `pysparky/functions/`: Functions that take a `Column` (or column name) and return a `Column`. They are categorized into domains like `strings/`, `math/`, `mappings/`, etc.
- `pysparky/transformations/`: Functions that take a `DataFrame` and return a `DataFrame`. They are categorized into `pipeline/`, `filtering/`, `aggregations/`, `sets/`, and `dedup/`.
- `pysparky/io/`: Helpers for reading/writing data formats (e.g., `csv/`).
- `pysparky/qa/`: Quality assurance tools like data validation rules and debuggers.
- `pysparky/schema/`: Schema extensions.
- `pysparky/utils_/`: Miscellaneous utilities.

For further instructions specific to categories, see the nested `AGENTS.md` files.
