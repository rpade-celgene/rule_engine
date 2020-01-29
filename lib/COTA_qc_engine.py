from pyspark.sql import functions as SQL
from pyspark.sql.types import *
from pyspark.sql.window import Window
from Base_qc_engine import Base_qc_engine
import sys

class COTA_qc_engine(Base_qc_engine):
  
  
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