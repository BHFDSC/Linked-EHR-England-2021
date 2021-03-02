# Databricks notebook source
# MAGIC %md # 7_dp_covariate_flags
# MAGIC  
# MAGIC **Description** Covariate records across GDPPR, HES -- diabetes and obesity
# MAGIC  
# MAGIC **Project(s)** Descriptive paper 
# MAGIC  
# MAGIC **Author(s)** Samantha Ip
# MAGIC  
# MAGIC **Reviewer(s)** Sam Hollings
# MAGIC  
# MAGIC **Date last updated** 2020-12-15, 13:30
# MAGIC  
# MAGIC **Date last reviewed** 2020-12-15
# MAGIC  
# MAGIC **Date last run** 2020-12-15, 13:30
# MAGIC  
# MAGIC **Data input** TBC
# MAGIC 
# MAGIC **Data output** Not applicable
# MAGIC 
# MAGIC **Software and versions** pyspark, python
# MAGIC  
# MAGIC **Packages and versions** Not applicable
# MAGIC 
# MAGIC **NOTES** check date column used, feature value column used, codelist

# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType


# COMMAND ----------

# MAGIC %md
# MAGIC ### read in databases
# MAGIC * GDPPR
# MAGIC * HES

# COMMAND ----------

# gdppr = spark.table('dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t')
# hes = spark.table('dars_nic_391419_j3w9t_collab.hes_apc_all_years')
# # depriv = spark.table('dss_corporate.eng_indices_of_dep_quantiles')
# depriv = spark.table('dss_corporate.english_indices_of_dep_v02')
# primary = spark.table('dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t')

gdppr = spark.table('global_temp.dp_gdppr')
hes = spark.table('global_temp.dp_hes_apc_all')
common_cutoff_date = to_date(lit("2020-10-31"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### functions

# COMMAND ----------

def list_medcodes(stringio_code_df, colname):
  if colname == "ICD10code":
    stringio_code_df[colname] = stringio_code_df[colname].str.replace('.', '', regex=True)
    
  stringio_code_df = stringio_code_df[colname].values
  
  codelist = [str(code) for code in stringio_code_df]
  return codelist

def mk_gdppr_covariate_flag(gdppr, prior_to_date_string, CONDITION_GDPPR_string, codelist, after_date_string=None):
    gdppr_tmp = gdppr.withColumnRenamed('NHS_NUMBER_DEID', 'ID').select(['ID', "DATE", "RECORD_DATE", "CODE"]).filter(col("DATE") < prior_to_date_string)
    if after_date_string:
      gdppr_tmp = gdppr_tmp.filter(col("DATE") > after_date_string)
      print(gdppr_tmp.agg({"DATE": "min"}).toPandas())
    gdppr_cov = gdppr_tmp.withColumn(CONDITION_GDPPR_string, when(col("CODE").cast('string').isin(codelist), 1).otherwise(0))
    gdppr_cov =  gdppr_cov.groupBy("ID").agg(f.max(CONDITION_GDPPR_string).alias(CONDITION_GDPPR_string))
    return gdppr_cov

def mk_hes_covariate_flag(hes, prior_to_date_string, CONDITION_HES_string, codelist, after_date_string=None):
  hes_tmp = hes.withColumnRenamed('PERSON_ID_DEID', 'ID').select(['ID', "EPISTART", "DIAG_4_CONCAT"]).filter(col("EPISTART") < prior_to_date_string)
  if after_date_string:
    hes_tmp = hes_tmp.filter(col("EPISTART") > after_date_string)
    print(hes_tmp.agg({"EPISTART": "min"}).toPandas())
#   in case of F05.1 --> F051
  hes_tmp = hes_tmp.withColumn("DIAG_4_CONCAT",f.regexp_replace(col("DIAG_4_CONCAT"), "\\.", ""))
  hes_cov = hes_tmp.where(
      reduce(lambda a, b: a|b, (hes_tmp['DIAG_4_CONCAT'].like('%'+code+"%") for code in codelist))
  ).select(["ID"]).withColumn(CONDITION_HES_string, lit(1))
  return hes_cov

def join_gdppr_hes_covariateflags(gdppr_cov, hes_cov, EVER_CONDITION_string, CONDITION_GDPPR_string, CONDITION_HES_string):
  df = gdppr_cov.join(hes_cov, "ID", "outer").na.fill(0)
  df = df.withColumn(EVER_CONDITION_string, when((col(CONDITION_GDPPR_string)+col(CONDITION_HES_string) )>0, 1).otherwise(0))
  return df
  
def examine_rows_column_value(df, colname, value):
    if value == None:
        tmp_df = df.where(col(colname).isNull())
    else:
        tmp_df = df[df[colname] == value]
    display(tmp_df)

    
def count_unique_pats(df, id_colname):
    n_unique_pats = df.agg(countDistinct(id_colname)).toPandas()
    return int(n_unique_pats.values)

def create_table(df, table_name:str, database_name:str="dars_nic_391419_j3w9t_collab", select_sql_script:str=None) -> None:
#   adapted from sam h 's save function
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")
  df.createOrReplaceGlobalTempView(table_name)
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}""")
  spark.sql(f"""
                ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}
             """)

# COMMAND ----------

# MAGIC %md ### 1) obesity 
# MAGIC * If ever recorded < 1st January 2020 by medcodes only

# COMMAND ----------

# codelists
obesity_icd10_codelist = spark.table(
    "bhf_cvd_covid_uk_byod.caliber_icd_obesity"
).toPandas()
obesity_snomed_codelist = spark.table(
    "bhf_cvd_covid_uk_byod.caliber_cprd_obesity_snomedct"
).toPandas()
obesity_icd10_codelist = list_medcodes(obesity_icd10_codelist, "ICD10code")
obesity_snomed_codelist = list_medcodes(obesity_snomed_codelist, "conceptId")

# COMMAND ----------

gdppr_obesity = mk_gdppr_covariate_flag(
    gdppr, "2020-01-01", "OBESE_GDPPR", obesity_snomed_codelist
)
hes_obesity = mk_hes_covariate_flag(
    hes, "2020-01-01", "OBESE_HES", obesity_icd10_codelist
)
df_obesity = join_gdppr_hes_covariateflags(
    gdppr_obesity,
    hes_obesity,
    "EVER_OBESE",
    "OBESE_GDPPR",
    "OBESE_HES",
)
display(df_obesity)

# COMMAND ----------

create_table(df_obesity, table_name="covariate_flags_obesity", database_name="dars_nic_391419_j3w9t_collab", select_sql_script=None)

# COMMAND ----------

# MAGIC %md ### 2) diabetes 
# MAGIC * if ever diagnosed < 1st Janurary 2020

# COMMAND ----------

# MAGIC %md ###### codelists

# COMMAND ----------

diabetes_icd10_codelist = spark.table('bhf_cvd_covid_uk_byod.caliber_icd_diabetes').toPandas()
diabetes_snomed_codelist = spark.table('bhf_cvd_covid_uk_byod.bhfcvdcovid_diabetes_snomed').toPandas()

diabetes_icd10_codelist = list_medcodes(diabetes_icd10_codelist, "ICD10code")
diabetes_snomed_codelist = list_medcodes(diabetes_snomed_codelist, "conceptId")

# COMMAND ----------

# MAGIC %md ###### make table

# COMMAND ----------

gdppr_diabetes = mk_gdppr_covariate_flag(
    gdppr, "2020-01-01", "OBESE_GDPPR", diabetes_snomed_codelist
)
hes_diabetes = mk_hes_covariate_flag(
    hes, "2020-01-01", "OBESE_HES", diabetes_icd10_codelist
)
df_diabetes = join_gdppr_hes_covariateflags(
    gdppr_diabetes,
    hes_diabetes,
    "EVER_DIAB",
    "DIAB_GDPPR",
    "DIAB_HES",
)
display(df_diab)

# COMMAND ----------

create_table(df_diab, table_name="covariate_flags_diabetes", database_name="dars_nic_391419_j3w9t_collab", select_sql_script=None)
