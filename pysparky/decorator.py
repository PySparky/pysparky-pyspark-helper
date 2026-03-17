import functools
from typing import Callable, Any
from pyspark.sql import DataFrame


def validate_columns(
    input_cols: list[str] = None,
    required_cols: list[str] = None,
    expected_cols: list[str] = None,
    output_cols: list[str] = None,
    added_cols: list[str] = None,
    dropped_cols: list[str] = None,
) -> Callable:
    """
    Decorator to validate the input and output columns of a PySpark DataFrame transformation.

    Args:
        input_cols (list[str], optional): The exact list of columns that must be present before transformation.
        required_cols (list[str], optional): The subset of columns that must be present before transformation.
        expected_cols (list[str], optional): The subset of columns that must be present after transformation.
        output_cols (list[str], optional): The exact list of columns that must be present after transformation.
        added_cols (list[str], optional): The exact list of newly added columns (output - input).
        dropped_cols (list[str], optional): The exact list of dropped columns (input - output).

    Returns:
        Callable: The decorated function.

    Examples:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, 100)], ["user_id", "raw_revenue"])
        >>> @validate_columns(required_cols=["user_id"], added_cols=["net_revenue"])
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
            in_columns = df.columns
            if input_cols is not None and in_columns != input_cols:
                raise ValueError(
                    f"Function {func.__name__} failed: Exact input columns {input_cols} did not match actual {in_columns}"
                )

            if required_cols is not None:
                missing_in = [c for c in required_cols if c not in in_columns]
                if missing_in:
                    raise ValueError(f"Function {func.__name__} failed: Missing required input columns {missing_in}")

            result = func(*args, **kwargs)

            # Post-transformation check
            if not isinstance(result, DataFrame):
                raise ValueError(f"Function {func.__name__} failed: Did not return a DataFrame")

            out_columns = result.columns
            if output_cols is not None and out_columns != output_cols:
                raise ValueError(
                    f"Function {func.__name__} failed: Exact output columns {output_cols} did not match actual {out_columns}"
                )

            if expected_cols is not None:
                missing_out = [c for c in expected_cols if c not in out_columns]
                if missing_out:
                    raise ValueError(f"Function {func.__name__} failed: Missing expected output columns {missing_out}")

            if added_cols is not None:
                actual_added = list(set(out_columns) - set(in_columns))
                if set(actual_added) != set(added_cols):
                    raise ValueError(
                        f"Function {func.__name__} failed: Added columns {actual_added} did not exactly match expected {added_cols}"
                    )

            if dropped_cols is not None:
                actual_dropped = list(set(in_columns) - set(out_columns))
                if set(actual_dropped) != set(dropped_cols):
                    raise ValueError(
                        f"Function {func.__name__} failed: Dropped columns {actual_dropped} did not exactly match expected {dropped_cols}"
                    )

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
