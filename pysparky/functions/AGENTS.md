# Functions Guidelines (`pysparky/functions/`)

This directory houses Column-to-Column operations.

## Rules
- **Categorization:** Do not create `general.py`. Group functions logically into sub-packages based on their domain. Current categories include:
  - `strings/`: String manipulations.
  - `mappings/`: Key-value or conditional mappings.
  - `math/`: Mathematical operations.
  - `type/`: Type casting.
  - `conditions/`: Boolean condition builders.
- **Inputs:** Many functions accept `ColumnOrName`. Use the helper `ensure_column` from `pysparky.core.enabler` to safely convert inputs to `Column` objects inside your function.
- **Exports:** When adding a new file or function, make sure to add its exports to `pysparky/functions/__init__.py`.
