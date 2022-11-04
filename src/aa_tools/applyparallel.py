import dask.dataframe as dd 
from os import cpu_count

def apply_parallel(df, func, **kwargs):
    """
    Apply a function on dataframes on multiple cores at same time.
    By default, uses the maximum available cores on your CPU.
    - Parameters of func() has to be passed and received as position arguments or keyword arguments.
    - This method will pass individual rows from dataframe to the func.

    Return
        Result as Series that can be stored in new column.
    """
    # Conversion to pandas Dataframe to Dask Dataframe
    ddf= dd.from_pandas(df, npartitions=cpu_count())

    # Apply function and store in Dask Series
    d_series = ddf.apply(func, args=kwargs, meta=df.dtypes, axis=1)

    return d_series.compute()