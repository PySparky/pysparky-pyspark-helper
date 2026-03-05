# Transformations Guidelines (`pysparky/transformations/`)

This directory houses DataFrame-to-DataFrame operations.

## Rules
- **Categorization:** Do not create `general.py`. Group transformations into logical sub-packages. Current categories include:
  - `pipeline/`: Transformations applied to sequences or blue-prints.
  - `filtering/`: Filtering rows or conditionally nullifying.
  - `aggregations/`: DataFrame aggregations.
  - `sets/`: Set operations like getting unique elements across dataframes.
  - `dedup/`: Deduplication functions.
- **Pure Functions:** Transformations should treat DataFrames as immutable. Return a newly transformed DataFrame.
- **Exports:** When adding a new file or transformation, make sure to add its exports to `pysparky/transformations/__init__.py`.
