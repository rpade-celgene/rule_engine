import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

from lib.ETLUtilities.Rules import UniquePKRule
from lib.dev_qc_func import *

def test_mark_duplicates():
    students = [('Jack', 34, 'Sydeny'),
                ('Riti', 30, 'Delhi'),
                ('Aadi', 16, 'New York'),
                ('Riti', 30, 'Mumbai'),
                ('Riti', 30, 'Delhi'),
                ('Riti', 30, 'Mumbai'),
                ('Aadi', 16, 'London'),
                ('Sachin', 30, 'Delhi')
                ]

    # Create a DataFrame object
    dfObj = pd.DataFrame(students, columns=['Name', 'Age', 'City'])

    dup_dict = {
        'students': ['Name', 'Age', 'City']}
    df_dict = {'students': dfObj}
    act_df = qc_dup(df_dict, dup_dict)
    rule_obj = UniquePKRule(df_dict['students'], dup_dict['students'])
    print(rule_obj.validation_result)
    print(act_df)
    pass

def test_mark_missing():
    students = [('Jack', 34, 'Sydeny'),
                ('Riti', 30, 'Delhi'),
                ('Aadi', np.nan, ),
                ('Riti', 30, 'Mumbai'),
                ('Riti', '   ', 'Delhi'),
                ('Riti', 30, ''),
                ('Aadi', None, 'London'),
                ('Sachin', 30, 'Delhi')
                ]

    # Create a DataFrame object
    dfObj = pd.DataFrame(students, columns=['Name', 'Age', 'City'])
    missing_dict = {
        'students': ['Age', 'City']}
    df_dict = {'students': dfObj}
    act_df = qc_missing(df_dict)
    print(act_df)
    pass

def test_check_value():
    students = [('Jack', 34, 'Sydeny', 72),
                ('Riti', 30, 'Delhi', 'N/A'),
                ('Aadi', "Dog", None, 60),
                ('Riti', 30, 'Mumbai', 74),
                ('Riti', -1, 'Delhi', -10),
                ('Riti', 30, '', 66),
                ('Aadi', '', 'London', 66),
                ('Sachin', 30, 'Delhi', -6)
                ]

    # Create a DataFrame object
    dfObj = pd.DataFrame(students, columns=['Name', 'value_Age', 'City', 'value_Height'])
    string_tables = {"value": "value", "tables": ['students']}
    df_dict = {'students': dfObj}
    act_df = qc_string_in_num(df_dict, string_tables)
    print(act_df)
    act_df = qc_negative_in_num(df_dict, string_tables)
    print(act_df)
    pass

def test_last_dt():
    students = [('Jack', 34, 'Sydeney', '2018/05/11', '2018/06/11'),
                ('Riti', 30, 'Delhi', '2018/05/06', '2017/05/06'),
                ('Aadi', 24, 'NYC', '2018/02/15', '2019/02/15'),
                ('Sachin', 30, 'Delhi', '2018/12/24', '2019/12/24')
                ]
    labs = [('Jack', 'advil',  '2019/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'advil', '2018/01/01', '2018/02/UK', ''),
            ('Aadi', 'advil', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'tynenol', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'concerta', '2018/10/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'nyquil', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Aadi', 'concerta', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Sachin', 'advil', '2018/01/01', '2018/02/10', '2019/04/10')
            ]
    # Create a DataFrame object
    df_students = pd.DataFrame(students, columns=['Name', 'value_Age', 'City', 'LASTDAT', 'DEATHDAT'])
    df_labs = pd.DataFrame(labs, columns=['Name', 'drug_name', 'FIRSTDAT', 'SECONDDAT', 'THIRDDAT'])
    df_dict = {'students': df_students, 'labs': df_labs}
    last_dt = {'table': 'students', 'dt_column': 'LASTDAT', 'id_column': 'Name', 'dt_value': 'DAT'}
    death_dt = {'table': 'students', 'dt_column': 'DEATHDAT', 'id_column': 'Name',  'dt_value': 'DAT'}
    act_df = qc_lastdate(df_dict, last_dt, death_dt)
    print(act_df)
    pass

def test_death_dt():
    students = [('Jack', 34, 'Sydeney', '2018/05/11', '2018/06/11'),
                ('Riti', 30, 'Delhi', '2018/05/06', '2017/05/06'),
                ('Aadi', 24, 'NYC', '2018/02/15', '2019/02/15'),
                ('Sachin', 30, 'Delhi', '2018/12/24', '2019/12/24')
                ]
    labs = [('Jack', 'advil',  '2019/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'advil', '2018/01/01', '2018/02/10', ''),
            ('Aadi', 'advil', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'tynenol', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'concerta', '2018/10/01', '2018/02/10', '2018/04/10'),
            ('Riti', 'nyquil', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Aadi', 'concerta', '2018/01/01', '2018/02/10', '2018/04/10'),
            ('Sachin', 'advil', '2018/01/01', '2018/02/10', '2019/04/10')
            ]
    # Create a DataFrame object
    df_students = pd.DataFrame(students, columns=['Name', 'value_Age', 'City', 'LASTDAT', 'DEATHDAT'])
    df_labs = pd.DataFrame(labs, columns=['Name', 'drug_name', 'FIRSTDAT', 'SECONDDAT', 'THIRDDAT'])
    df_dict = {'students': df_students, 'labs': df_labs}
    last_dt = {'table': 'students', 'dt_column': 'LASTDAT', 'id_column': 'Name', 'dt_value': 'DAT'}
    death_dt = {'table': 'students', 'dt_column': 'DEATHDAT', 'id_column': 'Name',  'dt_value': 'DAT'}
    act_df = qc_deathdate(df_dict, death_dt)
    print(act_df)
    pass

def test_freq():
    pass