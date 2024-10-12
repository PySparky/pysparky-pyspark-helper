# PySparky PySpark Helper
Welcome to the PySparky PySpark Helper repository! This Python package is designed to assist you in writing PySpark code more efficiently and effectively.

This repository is designed to replicate the structure of PySpark, making it highly accessible for users.

- The `functions` folder contains all PySpark functions, where both the input and output are Columns.
- The `Spark_ext`  houses functions that necessitate a Spark instance, such as creating a DataFrame.
- The `transformation_ext`  includes functions associated with DataFrame transformations, where both the input and output are DataFrames

# Build Process
```
pylint --generate-rcfile > .pylintrc
pylint pysparky
black .
isort .

pytest

python -m build
```

# TODO
- Change pytest test case
- Build wheels for PyPi

# Reference:
> https://github.com/apache/spark/tree/master/python/pyspark
> https://github.com/mrpowers-io/quinn
