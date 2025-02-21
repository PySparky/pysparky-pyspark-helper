echo "=== Black ==="
black .
echo "=== isort ==="
python -m isort .
# echo "=== pylint ==="
# pylint pysparky
echo "=== flake8 ==="
flake8 pysparky --max-line-length=115 --extend-ignore=E203,E501,E731
echo "=== mypy ==="
mypy .
echo "=== pytest ==="
pytest
echo "=== build ==="
python -m build
echo "=== mkdocs ==="
mkdocs serve -w docs
