from pyspark.sql import functions as SQL
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import Row
import sys, os
import subprocess
import pandas as pd
import numpy as np
import re
from custom_logging import log
from qc_util import *
import ETLUtilities.DataIO as DataIO
import ETLUtilities.Rules.UniquePKRule as UniquePKRule
from io import StringIO, BytesIO


class Base_qc_engine(object):

    @classmethod
    def init(cls, refresh_dt, input_loc, project, rules):
        cls.spec_year = refresh_dt
        cls.input_loc = project + "/input/"+ input_loc + "_" + re.sub('[-/]', '', refresh_dt) + "/"
        cls.output_loc = project + "/raw_qc/" + input_loc + "_" + re.sub('[-/]', '', refresh_dt) + "/"
        cls.rules = rules

    @classmethod
    def run_engine(cls, df_spark, rule_sequence):
        # apply generic rules

        df_result = cls.run_generic_rules(df_spark, rule_sequence)

        return df_result

    @classmethod
    def run_generic_rules(cls, df_spark, rule_sequence):
        result = df_spark
        for rule in rule_sequence:
            if rule == "duplication":
                result = cls.qc_dup(result, cls.rules["duplication"])
            elif rule == "missing":
                result = cls.qc_missing(result, cls.rules["missing"])
            elif rule == "string_tables":
                result = cls.qc_string_in_num(result, cls.rules["string_tables"])
            elif rule == "last_dt":
                result = cls.qc_lastdate(result, cls.rules["last_dt"])
            elif rule == "death_dt":
                result = cls.qc_deathdate(result, cls.rules["death_dt"])
        return result

    @classmethod
    def read_into_spark(cls):
        """
    Read dataset into spark dataframes given cofig specifications

    @input
    @output
    Dictionary of Dataframe
    """
        df_spark = {}
        fileList = []

        cmd = 'hdfs dfs -ls {}'.format(cls.input_loc + "*txt")

        fileList = [line.rsplit(None, 1)[-1] for line in
                    subprocess.check_output(cmd, shell=True, universal_newlines=True).split('\n') if
                    len(line.rsplit(None, 1))][1:]

        for filename in fileList:
            initials_filename = filename[filename.rfind('/') + 1:filename.find('.')]
            df_spark[initials_filename] = cls.spark.read.format("com.databricks.spark.csv").option("header",
                                                                                                   "true").option(
                "inferSchema", "true").option("delimiter", '|').load(filename)

        return df_spark

    def read_into_pandas(cls):
        """
    Read dataset into pandas dataframes given cofig specifications

    @input
    @output
    Dictionary of Dataframe
    """
        df_pandas = {}
        fileList = []

        cmd = 'ls {}'.format(cls.input_loc + "*txt")

        fileList = [line.rsplit(None, 1)[-1] for line in
                    subprocess.check_output(cmd, shell=True, universal_newlines=True).split('\n') if
                    len(line.rsplit(None, 1))][0:]

        for filename in fileList:
            initials_filename = filename[filename.rfind('/') + 1:filename.find('.')]
            bytes_data = DataIO.read(DataIO.RepoType.LOCAL, 'file://'+filename, read_type=DataIO.ReadType.BULK)
            df_pandas[initials_filename] = pd.read_csv(StringIO(bytes_data), sep="|", encoding="ISO-8859-1")
            #df_pandas[initials_filename] = pd.read_csv(filename, sep="|", encoding="ISO-8859-1")
            print(initials_filename)

        return df_pandas

    @classmethod
    def check_value_spark(cls, df_spark, string_tables):
        """
    Mark each row if incorrect values are found in columns that are supposed to numeric values

    @input
    spark -- SparkSession
    filename -- string 
    df_spark -- dictionary of spark dataframe
    string_tables -- list of tables to check for correct values
    @output
    Spark Dataframe
    """
        string_dict = {}
        for filename in string_tables:
            df = df_spark[filename]
            value_cols = [c for c in df.columns if 'value' in c]
            string_dict[filename] = value_cols
            tmp = df.select([(SQL.when(is_positive_digit(SQL.col(c)), 0).otherwise(1)).alias(c) for c in
                             string_dict[filename]])
            tmp = tmp.withColumn('total', sum(tmp[col] for col in tmp.columns)).select(
                SQL.when(SQL.col("total") > 0, 1).otherwise(0).alias("flag_values"))
            df = cls.add_column_index(df)
            tmp = cls.add_column_index(tmp)
            df_spark[filename] = df.join(tmp, on="columnindex").drop("columnindex")
        return df_spark

    @classmethod
    def mark_duplicates_spark(cls, df_spark, duplicate_dict):
        """
    Mark each row if duplicates found based on a primary key

    @input
    spark -- SparkSession
    filename -- string 
    df_spark -- dictionary of spark dataframe
    duplicate_dict -- dictionary of primary keys for each table
    @output
    Spark Dataframe
    """
        QC_LOG_FORMAT = '%(rule)s - %(total)s - %(count)s - %(percent)s'
        for filename in duplicate_dict:
            list = duplicate_dict[filename]
            df_spark[filename] = df_spark[filename].withColumn("count", SQL.count(list[0]).over(
                Window.partitionBy(list))).withColumn("flag_duplication",
                                                      SQL.when(SQL.col("count") > 1, 1).otherwise(0)).drop("count")
            dup_total = df_spark[filename].count()
            dup_count = df_spark[filename].where(SQL.col("flag_duplication") == 1).count()
            dup_percent = round(float(dup_count) / dup_total * 100, 2)
            message = "mark_duplicate - {} - {} - {} - {}".format(filename, dup_total, dup_count, dup_percent)
            log.info(message)
        return df_spark

    @classmethod
    def mark_missing_spark(cls, df_spark, missing_dict):
        """
    Mark a row if missing values in specified column

    @input
    df_spark -- dictionary of spark dataframe
    missing_dict -- dictionary of table, columns pair
    @output
    Dictionary of Spark Dataframe
    """
        for filename in df_spark:
            df = df_spark[filename]
            if filename in missing_dict:
                tmp = df.select([(SQL.when(SQL.isnan(c) | SQL.col(c).isNull(), 1).otherwise(0)).alias(c) for c in
                                 missing_dict[filename]])
            else:
                tmp = df.select(
                    [(SQL.when(SQL.isnan(c) | SQL.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
            tmp = tmp.withColumn('total', sum(tmp[col] for col in tmp.columns)).select(
                SQL.when(SQL.col("total") > 0, 1).otherwise(0).alias("flag_missing"))
            df = cls.add_column_index(df)
            tmp = cls.add_column_index(tmp)
            df_spark[filename] = df.join(tmp, on="columnindex").drop("columnindex")
        return df_spark

    @classmethod
    def add_column_index(cls, df):
        """
    Append a sequence of unique numbers as a column to a DF

    @input
    Spark Dataframe
    @output
    Spark Dataframe
    """

        row = Row(df.columns)
        row_with_index = Row(*["columnindex"] + df.columns)

        def make_row(columns):
            def _make_row(row, uid):
                row_dict = row.asDict()
                return row_with_index(*[uid] + [row_dict.get(c) for c in columns])

            return _make_row

        f = make_row(df.columns)
        df_with_pk = (df.rdd
                      .zipWithUniqueId()
                      .map(lambda x: f(*x))
                      .toDF(StructType([StructField("columnindex", LongType(), False)] + df.schema.fields)))
        return df_with_pk


    @classmethod
    def qc_dup(cls, dfs, dup_dict):
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
            rule_obj = UniquePKRule(dfs[name], cols)
            rule_obj.result_message(rule_obj.validation_result)
            rule_event_obj = rule_obj.result_to_event("QC_Engine", "001")
            if df_dup_cts.shape[0] != 0:
                print("\t >>>FAIL")
                key = name + ' : ' + ','.join(cols)
                df_dup_obs = df_dup_cts.merge(df, how='inner', on=cols)
                final_df = df_dup_cts.rename(columns={"counts": "dup_count"}).assign(dup_flag=1).merge(df, how='outer',
                                                                                                       on=cols).fillna(0)

                dup_total = final_df['dup_flag'].sum()
                total = len(final_df.index)
                dup_percent = round(float(dup_total) / total * 100, 2)

                report_dict[name] = df_dup_obs
                output_dict[name] = final_df
                dfs[name] = final_df
                csv_name = 'qc_dup_' + name + '.csv'
                report_path = os.path.join(cls.output_loc, csv_name)
                file_to_write = pdToStringIO(df_dup_obs)
                DataIO.write(DataIO.RepoType.LOCAL, 'file://'+report_path, content=file_to_write, query=None, username=None, password=None, write_type=DataIO.WriteType.NEW)
                #df_dup_obs.to_csv(report_path, index=False)

                message = "mark_duplicate - {} - {} - {} - {} - {}".format(name, total, dup_total, dup_percent, report_path)
                log.info(message)
                print("report location for {}: {}".format(name, report_path))

        print("...Done")
        return dfs

    @classmethod
    def qc_missing(cls, dfs, missing_dict=None, outdir=None):
        """
      Return the summary of missing values for all tables, all columns
      """
        print("\nCheck missing values...")
        df_list = []
        output_dict = {}

        for name, df in dfs.items():
            df_cols = list(df.columns)
            if name in missing_dict:
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
        file_to_write = pdToStringIO(df_qc_missing)
        DataIO.write(DataIO.RepoType.LOCAL, 'file://' + report_path, content=file_to_write, query=None, username=None,
                     password=None, write_type=DataIO.WriteType.NEW)

        #df_qc_missing.to_csv(report_path, index=False)
        message = "mark_missing - {}".format(report_path)
        log.info(message)
        print("report location: {}".format(report_path))
        print("...Done")
        return output_dict

    @classmethod
    def qc_string_in_num(cls, dfs, string_tables, outdir=None):
        """
        Check if there are string values in numeric fields. If there are NAs in the
        filed the function doesn't count.
        """

        print("\nCheck string values in numeric fields...")
        output_list = []
        string_dict = {}

        for name, df in dfs.items():
            print("\n" + name)
            cols = [c for c in df.columns if string_tables["value"] in c]
            print(cols)
        for name, df in dfs.items():
            df = dfs[name].copy()
            cols = [c for c in df.columns if string_tables["value"] in c]
            string_dict[name] = cols
            if not cols:
                continue
            for col in cols:
                col_num = col + '_num'
                df[col_num] = df[col]
                bool_df = df[col_num].astype(str).apply(lambda v: is_not_positive_digit(v))
                df[col_num] = bool_df
                strs = df.loc[bool_df, col].unique()
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

        report_path = os.path.join(cls.output_loc, 'qc_value.csv')
        file_to_write = pdToStringIO(output_df)
        DataIO.write(DataIO.RepoType.LOCAL, 'file://' + report_path, content=file_to_write, query=None, username=None,
                     password=None, write_type=DataIO.WriteType.NEW)

        #output_df.to_csv(report_path, index=False)
        message = "check_value - {}".format(report_path)
        log.info(message)
        print("report location: {}".format(report_path))
        print("...Done")
        return dfs

    @classmethod
    def qc_lastdate(cls, dfs, last_dt):
        """
                Check if there are date values in that are greater than last possible date for a patient based on last_dt specs.
        """

        print("\nCheck dates that come after last date values...")
        output_dict = {}
        dt_type = 'datetime64[ns]'
        if last_dt['dt_type'] == 'float':
            dt_type = 'float64'
        lastdt_df = dfs[last_dt['table']][[last_dt['id_column'], last_dt['dt_column']]].drop_duplicates()
        lastdt_df[last_dt['dt_column']] = lastdt_df[last_dt['dt_column']].astype(dt_type)
        for name, df in dfs.items():
            df = dfs[name].copy()
            if not last_dt["ignore_value"]:
                cols = [c for c in df.columns if last_dt["dt_value"] in c]
            else:
                cols = [c for c in df.columns if last_dt["dt_value"] in c and last_dt["ignore_value"] not in c]
            if last_dt['dt_column'] in cols:
                cols.remove(last_dt['dt_column'])
                df = df.drop(columns=last_dt['dt_column'], axis=1)
            if last_dt['ignorecols'] is not None:
                for col in last_dt['ignorecols']:
                    if col in cols:
                        cols.remove(col)
                        df = df.drop(columns=col, axis=1)
            if not cols:
                continue
            df[cols] = df[cols].replace('UK', '15', regex=True)
            for col in cols:
                df[col] = df[col].astype(dt_type)
            inter_df = df.merge(lastdt_df, how='outer', on=last_dt['id_column'])
            querystring = inequality_dt_query(last_dt['dt_column'], cols, '<', '|')
            df_last_dt_obs = inter_df.query(querystring, engine='python').assign(last_dt_flag=1)
            dfs[name] = inter_df.merge(df_last_dt_obs, how='outer', on=list(inter_df.columns))
            dfs[name]["last_dt_flag"] = dfs[name]["last_dt_flag"].fillna(0)
            dfs[name] = dfs[name].drop(columns=[last_dt['dt_column']], axis=1)

            if df_last_dt_obs.shape[0] != 0:
                print("\t Table: ", name, ">>>FAIL")
                output_dict[name] = df_last_dt_obs
                csv_name = 'qc_lastdt_' + name + '.csv'
                report_path = os.path.join(cls.output_loc, csv_name)
                file_to_write = pdToStringIO(df_last_dt_obs)
                DataIO.write(DataIO.RepoType.LOCAL, 'file://' + report_path, content=file_to_write, query=None,
                             username=None,
                             password=None, write_type=DataIO.WriteType.NEW)

                #f_last_dt_obs.to_csv(report_path, index=False)
                message = "qc_lastdate - {} - {}".format(name, report_path)
                log.info(message)
                print("report location for {}: {}".format(name, report_path))

        print("...Done")
        return dfs

    @classmethod
    def qc_deathdate(cls, dfs, death_dt):
        """
               Check if there are date values in that are greater than death date of a patient based on death_dt specs.
        """
        print("\nCheck dates that come after death date values...")
        output_dict = {}
        dt_type = 'datetime64[ns]'
        if death_dt['dt_type'] == 'float':
            dt_type = 'float64'
        deathdt_df = dfs[death_dt['table']][[death_dt['id_column'], death_dt['dt_column']]].drop_duplicates()
        deathdt_df[death_dt['dt_column']] = deathdt_df[death_dt['dt_column']].astype(dt_type)
        for name, df in dfs.items():
            df = dfs[name].copy()
            if not death_dt["ignore_value"]:
                cols = [c for c in df.columns if death_dt["dt_value"] in c]
            else:
                cols = [c for c in df.columns if death_dt["dt_value"] in c and death_dt["ignore_value"] not in c]
            if death_dt['dt_column'] in cols:
                cols.remove(death_dt['dt_column'])
                df = df.drop(columns=death_dt['dt_column'], axis=1)
            if death_dt['ignorecols'] is not None:
                for col in death_dt['ignorecols']:
                    if col in cols:
                        cols.remove(col)
                        df = df.drop(columns=col, axis=1)
            if not cols:
                continue
            df[cols] = df[cols].replace('UK', '15', regex=True)
            df[cols] = df[cols].astype(dt_type)
            inter_df = df.merge(deathdt_df, how='outer', on=death_dt['id_column'])
            querystring = inequality_dt_query(death_dt['dt_column'], cols, '<', '|')
            df_death_dt_obs = inter_df.query(querystring, engine='python').assign(death_dt_flag=1)
            dfs[name] = inter_df.merge(df_death_dt_obs, how='outer', on=list(inter_df.columns))
            dfs[name]["death_dt_flag"] = dfs[name]["death_dt_flag"].fillna(0)
            dfs[name] = dfs[name].drop(columns=[death_dt['dt_column']], axis=1)

            if df_death_dt_obs.shape[0] != 0:
                print("\t Table: ", name, ">>>FAIL")
                output_dict[name] = df_death_dt_obs
                csv_name = 'qc_deathdt_' + name + '.csv'
                report_path = os.path.join(cls.output_loc, csv_name)
                file_to_write = pdToStringIO(df_death_dt_obs)
                DataIO.write(DataIO.RepoType.LOCALcd, 'file://' + report_path, content=file_to_write, query=None,
                             username=None,
                             password=None, write_type=DataIO.WriteType.NEW)

                #df_death_dt_obs.to_csv(report_path, index=False)
                message = "qc_deathdate - {} - {}".format(name, report_path)
                log.info(message)
                print("report location for {}: {}".format(name, report_path))

        print("...Done")
        return dfs

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
    for name in string_tables:
        df = dfs[name].copy()
        cols = [c for c in df.columns if 'value' in c]
        string_dict[name] = cols
        df[cols] = df[cols].apply(lambda v: pd.to_numeric(v, errors='coerce'))
        df_neg_obs = df[(df[cols] < 0).any(1)]

        if df_neg_obs.shape[0] != 0:
            print("\t Table: ", name, ">>>FAIL")
            output_dict[name] = df_neg_obs
            #df_neg_obs.to_csv(os.path.join(outdir, 'neg.csv'), index=False)

    print("...Done")
    return output_dict
