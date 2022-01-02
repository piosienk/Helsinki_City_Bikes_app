import warnings
import functools


def not_used(func):
    """
    Decorator which can be used to mark functions
    as not used. Specifically applied to functions that calculates real distances between stations
    """

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn(
            "Call to alternative function version that was not tested and is not used in the algorithm {}.".format(
                func.__name__),
            category=DeprecationWarning,
            stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func
