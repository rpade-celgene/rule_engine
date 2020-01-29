import sys, os
import subprocess
import pandas as pd
import numpy as np
import re
from lib.custom_logging import log
from lib.qc_util import *


def qc_dup(dfs, dup_dict):
    """
  Check duplications for specific columns. Duplication is defined as multiple
  observations on the same day same patient.

  :param dict dfs: A dictionary that stores dataframe as the value and name
  as the key.
  :param dict dup_dict: Specify which tables and columns to check. Table name
  is as key and the list of duplicated keys as values.

  :return: print summary and csv output

  :example:
  dup_dict = {
      'Stem_Cell_Transplant': ['PATCOD','SCTYN','SCTDAT'],
      'Serum_M-Protein': ['PATCOD','LBDAT']}

  """
    print("\nCheck duplicated values...")

    output_dict = {}
    report_dict = {}
    for name, cols in dup_dict.items():
        print("\t Table: ", name)
        df = dfs[name]
        df_dup_cts = return_multiple_records(df, cols)

        if df_dup_cts.shape[0] != 0:
            print("\t >>>FAIL")
            print([df_dup_cts.shape[0], df.shape[0]])
            key = name + ' : ' + ','.join(cols)
            df_dup_obs = df_dup_cts.merge(df, how='inner', on=cols)
            final_df = df_dup_cts.rename(columns={"counts": "dup_count"}).assign(dup_flag=1).merge(df, how='outer',
                                                                                                   on=cols).fillna(0)
            dup_total = final_df['dup_flag'].sum()
            total = len(final_df.index)
            dup_percent = round(float(dup_total) / total * 100, 2)

            report_dict[name] = df_dup_obs
            output_dict[name] = final_df

            csv_name = 'qc_dup_' + name + '.csv'
            #report_path = os.path.join(cls.output_loc, csv_name)
            #df_dup_obs.to_csv(report_path, index=False)

            #message = "mark_duplicate - {} - {} - {} - {} - {}".format(name, total, dup_total, dup_percent, report_path)
            #log.info(message)
            log.info(df_dup_obs)

    print("...Done")
    return output_dict


def qc_missing(cls, dfs, missing_dict=None, outdir=None):
    """
  Return the summary of missing values for all tables, all columns
  """
    print("\nCheck missing values...")
    df_list = []
    output_dict = {}

    for name, df in dfs.items():
        df_cols = list(df.columns)
        if missing_dict:
            cols = missing_dict[name]
        else:
            cols = df_cols
        filterdf = df.replace(r'^\s*$', np.nan, regex=True).dropna(axis=0, how='any', subset=cols).assign(missing_flag=0)
        filterdf = filterdf.merge(df, how='outer', on=df_cols)
        filterdf["missing_flag"] = filterdf["missing_flag"].fillna(1)
        output_dict[name] = filterdf
        df = (check_missing_per_tbl(df, cols)
              .reset_index(drop=False)
              .assign(tbl=name)
              .rename(columns={'index': 'column'}))
        df_list.append(df)


    df_qc_missing = pd.concat(df_list)
    report_path = os.path.join(cls.output_loc, 'qc_missing.csv')

    df_qc_missing.to_csv(report_path, index=False)
    message = "mark_missing - {}".format(report_path)
    log.info(message)
    print("...Done")
    print("report: ")
    print(df_qc_missing)
    return (output_dict)


def qc_string_in_num(dfs, string_tables, outdir=None):
    """
    Check if there are string values in numeric fields. If there are NAs in the
    filed the function doesn't count.
    """

    print("\nCheck string values in numeric fields...")
    output_list = []
    string_dict = {}
    for name in string_tables["tables"]:
        df = dfs[name].copy()
        cols = [c for c in df.columns if string_tables["value"] in c]
        string_dict[name] = cols
        for col in cols:
            col_num = col + '_num'
            df[col_num] = df[col]
            bool_df = df[col_num].astype(str).apply(lambda v: is_not_positive_digit(v))
            df[col_num] = bool_df
            strs = df.loc[bool_df, col].unique()
            print(strs)
            strs = [s for s in strs if s == s]

            if len(strs) != 0:
                output_list.append([name, col, strs])
        cols_num = (s + '_num' for s in cols)
        df = df[df.select_dtypes([bool]).any(1)].drop(columns=cols_num, axis=1).assign(value_flag=1)
        dfs[name] = dfs[name].merge(df, how='outer', on=list(dfs[name].columns))
        dfs[name]["value_flag"] = dfs[name]["value_flag"].fillna(0)
    output_df = pd.DataFrame(
        output_list,
        columns=['tbl', 'column', 'strings'])
    print(output_df)
    #report_path = os.path.join(cls.output_loc, 'string.csv')
    #output_df.to_csv(report_path, index=False)
    #message = "check_value - {}".format(report_path)
    #log.info(message)
    print("...Done")
    return (dfs)


def return_multiple_records(df, keylist):
    """
    Check if there are multiple records per key1-key2. E.g. check if there are
    multiple lab records for the same patient on the same day.
    :param dataframe df:
    :param list keylist: a list of column names as aggregation keys
    :return int: The number of groups that have duplicated result.
    """

    df_count = df.groupby(keylist).size().reset_index(name='counts')
    df_dup = df_count[df_count.counts >= 2].sort_values(keylist)
    print(df_dup.assign(dup_flag=1))

    return (df_dup)


def check_missing_per_tbl(df, cols=None):
    """
  """
    if cols == None:
        cols = df.columns

    df_missing_cts = pd.DataFrame(
        df[cols].replace(r'^\s*$', np.nan, regex=True).isnull().sum(),
        columns=['missing'])

    return (df_missing_cts)


def qc_negative_in_num(dfs, string_tables, outdir=None):
    """
    """
    print("\nCheck negative values in numeric fields...")

    output_dict = {}
    string_dict = {}
    for name in string_tables["tables"]:
        df = dfs[name].copy()
        cols = [c for c in df.columns if string_tables["value"] in c]
        string_dict[name] = cols
        df[cols] = df[cols].apply(lambda v: pd.to_numeric(v, errors='coerce'))
        df_neg_obs = df[(df[cols] < 0).any(1)]

        if df_neg_obs.shape[0] != 0:
            print("\t Table: ", name, ">>>FAIL")
            output_dict[name] = df_neg_obs
            #df_neg_obs.to_csv(os.path.join(outdir, 'neg.csv'), index=False)

    print("...Done")
    return (output_dict)


def qc_lastdate(dfs, last_dt, death_dt=None):
    output_dict = {}
    string_dict = {}

    lastdt_df = dfs[last_dt['table']][[last_dt['id_column'], last_dt['dt_column']]].drop_duplicates()
    lastdt_df[last_dt['dt_column']] = lastdt_df[last_dt['dt_column']].astype('datetime64[ns]')
    for name, df in dfs.items():
        df = dfs[name].copy()
        cols = [c for c in df.columns if last_dt["dt_value"] in c]
        if last_dt['dt_column'] in cols:
            cols.remove(last_dt['dt_column'])
            df = df.drop(columns=last_dt['dt_column'], axis=1)
        if death_dt is not None:
            if death_dt['dt_column'] in cols:
                cols.remove(death_dt['dt_column'])
                df = df.drop(columns=death_dt['dt_column'], axis=1)
        if not cols:
            continue
        #df[cols] = df[cols].fillna('1701/01/01').astype('datetime64[ns]')
        #df[cols] = df[cols].astype('datetime64[ns]', errors="ignore")
        df[cols] = df[cols].replace('UK', '15', regex=True)
        df[cols] = df[cols].apply(lambda v: pd.to_datetime(v, errors="coerce"))
        inter_df = df.merge(lastdt_df, how='outer', on=last_dt['id_column'])
        querystring = inequality_dt_query(last_dt['dt_column'], cols, '<', '|')
        df_last_dt_obs = inter_df.query(querystring, engine='python').assign(last_dt_flag=1)
        dfs[name] = inter_df.merge(df_last_dt_obs, how='outer', on=list(inter_df.columns))
        dfs[name]["last_dt_flag"] = dfs[name]["last_dt_flag"].fillna(0)

        if df_last_dt_obs.shape[0] != 0:
            print("\t Table: ", name, ">>>FAIL")
            output_dict[name] = df_last_dt_obs
            #df_last_dt_obs.to_csv(os.path.join(outdir, 'neg.csv'), index=False)

    print("...Done")
    return (dfs)
    pass

def qc_deathdate(dfs, death_dt):
    output_dict = {}
    string_dict = {}

    deathdt_df = dfs[death_dt['table']][[death_dt['id_column'], death_dt['dt_column']]].drop_duplicates()
    deathdt_df[death_dt['dt_column']] = deathdt_df[death_dt['dt_column']].astype('datetime64[ns]')
    for name, df in dfs.items():
        df = dfs[name].copy()
        cols = [c for c in df.columns if death_dt["dt_value"] in c]
        if death_dt['dt_column'] in cols:
            cols.remove(death_dt['dt_column'])
            df = df.drop(columns=death_dt['dt_column'], axis=1)
        if not cols:
            continue
        #df[cols] = df[cols].fillna('1701/01/01').astype('datetime64[ns]')
        df[cols] = df[cols].astype('datetime64[ns]')
        inter_df = df.merge(deathdt_df, how='outer', on=death_dt['id_column'])
        querystring = inequality_dt_query(death_dt['dt_column'], cols, '<', '|')
        df_death_dt_obs = inter_df.query(querystring).assign(death_dt_flag=1)


        if df_death_dt_obs.shape[0] != 0:
            print("\t Table: ", name, ">>>FAIL")
            output_dict[name] = df_death_dt_obs
            # df_neg_obs.to_csv(os.path.join(outdir, 'neg.csv'), index=False)

    print("...Done")
    return (output_dict)
    pass

def inequality_dt_query(dt_col, cols, inequality, operator):
    string = None
    for col in cols:
        if string:
            string = string + ' {} {} {} {}'.format(operator, dt_col, inequality, col)
        else:
            string = '{} {} {}'.format(dt_col, inequality, col)
    return string