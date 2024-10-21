echo "=== Black ==="
black .
echo "=== isort ==="
python -m isort .
echo "=== pylint ==="
pylint pysparky
echo "=== mypy ==="
mypy .
echo "=== pytest ==="
pytest
echo "=== build ==="
python -m build
echo "=== mkdocs ==="
mkdocs serve -w docs