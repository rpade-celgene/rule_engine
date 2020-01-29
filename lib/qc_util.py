import numpy as np
from io import StringIO, BytesIO

def is_positive_digit(s):
    """ Returns True is string is a number. """
    if s in [None,  np.nan]:
        return False
    return s.replace('.', '', 1).isdigit()

def is_not_positive_digit(s):
    """ Returns True is string is a number. """
    if s in [None,  np.nan]:
        return True
    return not s.replace('.', '', 1).isdigit()

def inequality_dt_query(dt_col, cols, inequality, operator):
    """ Returns query string for comparing a column with a list of columns"""
    string = None
    for col in cols:
        if string:
            string = string + ' {} {} {} {}'.format(operator, dt_col, inequality, col)
        else:
            string = '{} {} {}'.format(dt_col, inequality, col)
    return string

def pdToStringIO(pd_df):
    towrite = StringIO()
    pd_df.to_csv(towrite, index=False)
    towrite.seek(0)
    file_to_write = towrite.read()
    return file_to_write