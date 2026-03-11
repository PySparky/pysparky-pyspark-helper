import functools
from typing import Callable, Any
from pyspark.sql import DataFrame


def validate_schema(inputs: list[str], outputs: list[str]) -> Callable:
    """
    Decorator to validate the input and output schema of a PySpark DataFrame transformation.

    This ensures that the DataFrame has the required input columns before the
    function runs and that the required output columns are present after it runs.

    Args:
        inputs (list[str]): List of column names that must be present in the DataFrame before transformation.
        outputs (list[str]): List of column names that must be present in the DataFrame after transformation.

    Returns:
        Callable: The decorated function.

    Examples:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, 100)], ["user_id", "raw_revenue"])
        >>> @validate_schema(inputs=["user_id", "raw_revenue"], outputs=["net_revenue"])
        ... def calculate_net_revenue(df: DataFrame) -> DataFrame:
        ...     return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)
        >>> result = calculate_net_revenue(df)
        >>> result.columns
        ['user_id', 'raw_revenue', 'net_revenue']
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Find the DataFrame argument
            df = None
            for arg in args:
                if isinstance(arg, DataFrame):
                    df = arg
                    break
            if df is None:
                for kwarg in kwargs.values():
                    if isinstance(kwarg, DataFrame):
                        df = kwarg
                        break

            if df is None:
                raise ValueError(f"Function {func.__name__} failed: No DataFrame argument found")

            # Pre-transformation check
            missing_in = [c for c in inputs if c not in df.columns]
            if missing_in:
                raise ValueError(f"Function {func.__name__} failed: Missing input columns {missing_in}")

            result = func(*args, **kwargs)

            # Post-transformation check
            if not isinstance(result, DataFrame):
                raise ValueError(f"Function {func.__name__} failed: Did not return a DataFrame")

            missing_out = [c for c in outputs if c not in result.columns]
            if missing_out:
                raise ValueError(f"Function {func.__name__} failed: Expected output columns {missing_out} missing")

            return result
        return wrapper
    return decorator


def extension_enabler(cls):
    """
    This enable you to chain the class
    """

    def decorator(func):
        # assign the function into the object
        setattr(cls, f"{func.__name__}", func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_return = func(*args, **kwargs)
            return func_return

        return wrapper

    return decorator
