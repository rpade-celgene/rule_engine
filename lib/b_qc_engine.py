from pyspark.sql import functions as SQL
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import Row
import sys, os
import subprocess
import re
from custom_logging import log

class Base_qc_engine(object):

  @classmethod
  def init(cls, spark, refresh_dt, input_loc, rules):
    cls.spark = spark
    cls.spec_year = refresh_dt
    cls.input_loc = input_loc + "_"+ re.sub('[-/]', '', refresh_dt) + "/input/"
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
        result = cls.mark_duplicates(result, cls.rules["duplication"])
      elif rule == "missing":
        result = cls.mark_missing(result, cls.rules["missing"])
      elif rule =="string_tables":
        result = cls.check_value(result, cls.rules["string_tables"])

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
  
  
    cmd = 'hdfs dfs -ls {}'.format(cls.input_loc)
  
    fileList = [ line.rsplit(None,1)[-1] for line in subprocess.check_output(cmd, shell=True, universal_newlines=True).split('\n') if len(line.rsplit(None,1))][1:]

    for filename in fileList:
      initials_filename = filename[filename.rfind('/')+1:filename.find('.')]
      df_spark[initials_filename] = cls.spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(filename)
      
    return df_spark
  

  @classmethod  
  def check_value(cls,df_spark, string_tables):
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
      tmp = df.select([(SQL.when(cls.is_number_repl_isdigit(SQL.col(c)), 0).otherwise(1)).alias(c) for c in string_dict[filename]])
      tmp = tmp.withColumn('total', sum(tmp[col] for col in tmp.columns)).select(SQL.when(SQL.col("total") > 0, 1).otherwise(0).alias("flag_values"))
      df = cls.add_column_index(df)
      tmp = cls.add_column_index(tmp)
      df_spark[filename] = df.join(tmp , on="columnindex").drop("columnindex")      
    return df_spark


  @classmethod  
  def mark_duplicates(cls,df_spark, duplicate_dict):
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
      df_spark[filename] = df_spark[filename].withColumn("count", SQL.count(list[0]).over(Window.partitionBy(list))).withColumn("flag_duplication", SQL.when(SQL.col("count") > 1, 1).otherwise(0)).drop("count")
      dup_total = df_spark[filename].count()
      dup_count = df_spark[filename].where(SQL.col("flag_duplication") == 1).count()
      dup_percent = round(float(dup_count) / dup_total * 100, 2)
      message = "mark_duplicate - {} - {} - {} - {}".format(filename, dup_total, dup_count, dup_percent)
      log.info(message)
    return df_spark

  
  @classmethod  
  def mark_missing(cls, df_spark, missing_dict):
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
        tmp = df.select([(SQL.when(SQL.isnan(c) | SQL.col(c).isNull(), 1).otherwise(0)).alias(c) for c in missing_dict[filename]])
      else:
        tmp = df.select([(SQL.when(SQL.isnan(c) | SQL.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
      tmp = tmp.withColumn('total', sum(tmp[col] for col in tmp.columns)).select(SQL.when(SQL.col("total") > 0, 1).otherwise(0).alias("flag_missing"))
      df = cls.add_column_index(df)
      tmp = cls.add_column_index(tmp)    
      df_spark[filename] = df.join(tmp , on="columnindex").drop("columnindex")
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
  def is_number_repl_isdigit(s):
    """ Returns True is string is a number. """
    return s.replace('.','',1).isdigit()