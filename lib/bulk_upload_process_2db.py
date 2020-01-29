from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext # SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os
import subprocess
import json  
import argparse 
import pandas as pd
import re
import time

BASE_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
QC_LOG_FORMAT = '%(rule)s - %(total)s - %(count)s - %(percent)s'

def parseArguments():
  parser = argparse.ArgumentParser(description="""Run QC Rule Engine
      """)

  # Optional arguments (but really required)
  parser.add_argument("-y", "--spec-year", required=True, dest="spec_year", type=int, help="Vendor Spec Year YYYY")
  parser.add_argument("-v", "--vendors", required=True, dest="vendors_in", type=str, default="", help="comma separate list of vendor to run")

  args = parser.parse_args()

  return args

def upload_via_pyspark(spark, input_csv, output_tb):
    """
    Upload datafarme to Impala via Spark
    
    @input 
    input_csv(str)-- e.g. "test.csv"
    output_tb(str) -- e.g. "iku_work.yl_table_name", make sure to have database name

    @output
    None

    @example 
    ####TODO
    In terminal: 
    spark-submit --executor-memory 14g --packages com.databricks:spark-csv_2.10:1.4.0 rule_engine/lib/bulk_upload_process_2db.py -v "grn" -y "2019" | tee date +"rule_engine/log/log_%Y%m%d.txt"


    spark-submit upload2db.py testfile.csv iku_work.yl_tmp 
    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 clean_b_upload2db.py file test.csv iku_ds_cur_connectmm_dara.y3_tmp
    
    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 bulk_b_upload2db.py folder connect_mm iku_ds_cur_connectmm_dara

    """
    
    print("\nStart uploading dataframe to Impala via Spark...")
    print("\n"+output_tb)
    print("#################################################")
    
    print("\Dropping Table uploading dataframe to Impala via Spark...")
    print("#################################################")
    spark.sql("DROP TABLE IF EXISTS " + output_tb)

    #spark = setup_spark_connection()
    # import file
    
    #df_spark = spark.read.csv(input_csv, header = True)
    df_spark = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(input_csv)


    
    tmp = "tmp100"
    df_spark.registerTempTable(tmp)
    # hsc.sql(s"""
    # CREATE TABLE $tableName (
    # // field definitions   )cd 
    # STORED AS $format """)
    
    spark.sql("CREATE TABLE " + output_tb +" AS SELECT * FROM " + tmp)
    # upload

    print("...Done")
    print("################################################\n")


def upload_to_hive(spark, df, filename, output_db):
    """
    Upload datafarme to Impala via Spark
    
    @input 
    spark -- Spark Session with Hive Context
    df -- Spark Dataframe
    filename(str)-- e.g. "test"
    output_db(str) -- e.g. "iku_work", make sure to have database name

    @output
    None

    @example 
    ####TODO
    In terminal: 
    spark-submit --executor-memory 14g --driver-memory 10 --packages com.databricks:spark-csv_2.10:1.4.0 rule_engine/lib/bulk_upload_process_2db.py -v "grn" -y "2019"

    spark-submit upload2db.py testfile.csv iku_work.yl_tmp 
    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 clean_b_upload2db.py file test.csv iku_ds_cur_connectmm_dara.y3_tmp
    
    spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 bulk_b_upload2db.py folder connect_mm iku_ds_cur_connectmm_dara

    """
    output_tb = output_db + "." + filename
    print("\nStart uploading dataframe to Impala via Spark...")
    print("\n"+output_tb)
    print("#################################################")
    
    print("\Dropping Table uploading dataframe to Impala via Spark...")
    print("#################################################")
    spark.sql("DROP TABLE IF EXISTS " + output_tb)

    
    tmp = "tmp300"
    df.registerTempTable(tmp)

    
    spark.sql("CREATE TABLE " + output_tb +" AS SELECT * FROM " + tmp)
    # upload

    print("...Done")
    print("################################################\n")


def run_GRN(spark, config):

    from GRN_qc_engine import GRN_qc_engine
    GRN = GRN_qc_engine()

    vendor_data = {      
    }
    ts = time.gmtime()
    cur_time = time.strftime("_%Y%m%d%H%M%S", ts)
    GRN.init(spark, config["refresh_dt"], config["folder"], config["rules"])
    vendor_data =  GRN.read_into_spark()
    for filename in vendor_data:
        upload_to_hive(spark, vendor_data[filename], filename + "_RAW", config["database"])
    result = GRN.run_engine( (vendor_data), config["rule_sequence"])
    
    output_loc = config["folder"] + "_" +  re.sub('[-/]', '', config["refresh_dt"]) + "/output/"
    for filename in result:
        result[filename].coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output_loc  + filename + cur_time + ".csv")
        upload_to_hive(spark, result[filename], filename + "_QC", config["database"])
        

def run_COTA(spark, config):

    from COTA_qc_engine import COTA_qc_engine
    COTA = COTA_qc_engine()

    vendor_data = {      
    }

    COTA.init(spark, config["refresh_dt"], config["folder"], config["rules"])
    vendor_data = COTA.read_into_spark()
    for filename in vendor_data:
        upload_to_hive(spark, vendor_data[filename], filename + "_RAW", config["database"])
    result = COTA.run_engine( (vendor_data), config["rule_sequence"])
    
    output_loc = config["folder"] + "_" +  re.sub('[-/]', '', config["refresh_dt"]) + "/output/"
    for filename in result:
        result[filename].coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output_loc  + filename + ".csv")
        upload_to_hive(spark, result[filename], filename + "_QC", config["database"])
        
        
def run_Base(spark, config):

    from Base_qc_engine import Base_qc_engine
    Base = Base_qc_engine()

    vendor_data = {      
    }

    
    Base.init(spark, config["refresh_dt"], config["folder"], config["rules"])
    vendor_data = Base.read_into_spark()
    for filename in vendor_data:
        upload_to_hive(spark, vendor_data[filename], filename + "_RAW", config["database"])
    result = Base.run_engine( (vendor_data), config["rule_sequence"])
    
    output_loc = config["folder"] + "_" +  re.sub('[-/]', '', config["refresh_dt"]) + "/output/"
    for filename in result:
        result[filename].coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output_loc  + filename + ".csv")
        upload_to_hive(spark, result[filename], filename + "_QC", config["database"])

    
def setup_spark_connection():
    """
    Set up Spark connection

    @input
    None
    @output
    sqlconn(a HiveContext object) --
    """

    # check Spark Expiration date

    SparkContext.setSystemProperty("hive.metastore.uris", "thrift://z9awsspsyn2m52.celgene.com:9083")

    # set up connection
    sparkconf = SparkConf().setAppName("upload_data")
    sc = SparkContext(conf=sparkconf)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel( logger.Level.ERROR )
    sqlconn = HiveContext(sc)

    return(sqlconn)
    


if __name__ == "__main__":
    args = parseArguments()
    
    run_list = args.vendors_in.split(",") # convert measures into list.
    spark = setup_spark_connection()  # setup connection for upload
    config_file = "rule_engine/conf/config" ".json"
    config = {}
    try:
      with open(config_file, 'r') as infile:
        config = json.load(infile)
    except Exception as e:
      print("Failed to open configuration file: "  + str(e))
      sys.exit(1)
      
    output_version = config["spec_version"]
    print ("OUT Version: " + str(output_version))
    
    for vendor in run_list:
        print("Running: " + vendor)
        if vendor == "grn":
            run_GRN(spark, config["GRN"])
        elif vendor == "cota":
            run_COTA(spark, config["COTA"])
        elif vendor.upper() in config:
            run_Base(spark, config[vendor.upper()])
        else:
            print(vendor + ":: No rule engine for the vendor!")
        print(vendor + " complete")
    
        



