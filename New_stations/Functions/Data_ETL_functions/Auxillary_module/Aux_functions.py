import warnings
import functools
import pandas as pd


def check_columns(path_to_file):
    df = pd.read_csv(path_to_file)
    needed_cols = ['distance (m)', 'duration (sec.)', 'avg_speed (km/h)', 'departure', 'return',
                   'departure_name', 'return_name', 'departure_latitude', 'departure_longitude',
                   'return_latitude', 'return_longitude']

    for col in needed_cols:
        if col in df.columns:
            continue
        else:
            raise ValueError('Needed columns not present in new data')
    return

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
