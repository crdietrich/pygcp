"""Utility Functions for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""


import pandas as pd


def is_notebook() -> bool:
    """Determine what frontend is being used.

    Taken from the Stack Overflow comment by Gustavo Bezerra:
    https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook

    Returns
    -------
    bool
        True if in Jupyter Notebook
        False in all other cases
    """

    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def null_count(_df):
    """Count and calculate the percentage of null values in all columns

    Parameters
    ----------
    _df : Pandas DataFrame

    Returns
    -------
    Pandas DataFrame with columns
        column : name of column in input DataFrame
        null_count : int, count of nulls in the column
        null_percent : float, percentage of nulls in the column
    """
    _dfn = pd.DataFrame({"column": _df.columns, "null_count": _df.isnull().sum()})
    _dfn["null_percent"] = 100 * _dfn.null_count / len(_df)
    _dfn.sort_values(by="null_percent", inplace=True)
    _dfn.reset_index(inplace=True, drop=True)
    return _dfn


def null_plot(_df, metric="percent", figsize=(10, 10)):
    """Plot the null count in a Pandas DataFrame

    Parameters
    ----------
    _df : Pandas DataFrame to compute the null count in
    metric : str, if 'percent' plot percent nulls or
        if 'count' plot the count of nulls
    figsize : tuple of int, matplotlib figsize, default is (10,10)
    """
    _dfn = null_count(_df)
    y_col = "null_" + metric
    _dfn.plot(x="column", y=y_col, kind="barh", color="grey", figsize=figsize, rot=0)
