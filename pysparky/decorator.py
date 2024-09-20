import functools

from pyspark.sql import functions as F


def pyspark_column_or_name_enabler(*param_names):
    """
    A decorator to enable PySpark functions to accept either column names (as strings) or Column objects.

    Parameters:
    param_names (str): Names of the parameters that should be converted from strings to Column objects.

    Returns:
    function: The decorated function with specified parameters converted to Column objects if they are strings.

    Example
    @pyspark_column_or_name_enabler("column_or_name")
    def your_function(column_or_name):
        return column_or_name.startswith(bins)
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Convert args to a list to modify them
            # args: This is the list of argument of the function.
            # Get the parameter indices from the function signature
            # func.__code__.co_varnames : Return the function parameter names as a tuple.
            # param_names : the list of parameter from the decorator

            # Merging the args into Kwargs
            args_name_used = func.__code__.co_varnames[: len(args)]
            kw_from_args = dict(zip(args_name_used, args))
            kwargs = kw_from_args | kwargs

            # print(kwargs)
            # transform all the input param
            for param_name in param_names:
                # if it is string, wrap it as string, else do nth
                kwargs[param_name] = (
                    F.col(kwargs[param_name])
                    if isinstance(kwargs[param_name], str)
                    else kwargs[param_name]
                )

            return func(**kwargs)

        return wrapper

    return decorator


def column_name_or_column_names_enabler(*param_names):
    """
    A decorator to enable PySpark functions to accept either column names (as strings) or Column objects.

    Parameters:
    param_names (str): Names of the parameters that should be converted from strings to Column objects.

    Returns:
    function: The decorated function with specified parameters converted to Column objects if they are strings.

    Example
    @pyspark_column_or_name_enabler("column_or_name")
    def your_function(column_or_name):
        return column_or_name.startswith(bins)
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Convert args to a list to modify them
            # args: This is the list of argument of the function.
            # Get the parameter indices from the function signature
            # func.__code__.co_varnames : Return the function parameter names as a tuple.
            # param_names : the list of parameter from the decorator

            # Merging the args into Kwargs
            args_name_used = func.__code__.co_varnames[: len(args)]
            kw_from_args = dict(zip(args_name_used, args))
            kwargs = kw_from_args | kwargs

            # print(kwargs)
            # transform all the input param
            for param_name in param_names:
                # if it is string, wrap it as string, else do nth
                kwargs[param_name] = (
                    [kwargs[param_name]]
                    if isinstance(kwargs[param_name], str)
                    else kwargs[param_name]
                )

            return func(**kwargs)

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
