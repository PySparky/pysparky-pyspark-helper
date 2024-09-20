# pyspark-helper
python package that assist you writing pyspark

This repository is designed to replicate the structure of PySpark, making it highly accessible for users.

- The `functions` folder contains all PySpark functions, where both the input and output are Columns.
- The `Spark_ext`  houses functions that necessitate a Spark instance, such as creating a DataFrame.
- The `transformation_ext`  includes functions associated with DataFrame transformations, where both the input and output are DataFrames


# Lint
```
pylint --generate-rcfile > .pylintrc
pylint pysparky
black .
isort .

pytest
```

# TODO
- Change pytest test case
- Change the visibility to Public
- Build mkdocs
- Build wheels for PyPi
