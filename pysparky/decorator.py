import functools


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
