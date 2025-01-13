def format_array_for_sql(array):
    """
    Formats an array to be used in an SQL _query.

    Parameters
    ----------
    array : list or tuple
        The array to format.

    Returns
    -------
    str
        The formatted array as a string.
    """
    return ", ".join(list(array))