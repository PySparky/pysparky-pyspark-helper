# IO Guidelines (`pysparky/io/`)

This directory contains helpers for reading and writing Spark DataFrames across different formats.

## Rules
- **Formats:** Place specific format logic inside a dedicated sub-package (e.g., `csv/`).
- **Standardization:** Ensure reader and writer functions support common configurations (modes, headers, options) natively supported by PySpark.
