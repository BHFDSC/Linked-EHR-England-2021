# Databricks notebook source
# MAGIC %md
# MAGIC  # 8_dp_deaths: Deaths with cause of death including COVID
# MAGIC  
# MAGIC **Description** Death with cause of death including COVID medcodes and stats for associated phenotypes
# MAGIC 
# MAGIC **Project(s)** Descriptive Manuscript
# MAGIC  
# MAGIC **Author(s)** Samantha Ip
# MAGIC  
# MAGIC **Reviewer(s)** Sam Hollings
# MAGIC  
# MAGIC **Date last updated** 2020-12-23
# MAGIC  
# MAGIC **Date last reviewed** 2020-02-10
# MAGIC  
# MAGIC **Date last run** 2020-12-23
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC **Software and versions** 
# MAGIC  
# MAGIC **Packages and versions** 

# COMMAND ----------

from pyspark.sql.functions import countDistinct, min, max, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between, col
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
from functools import reduce
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### define functions

# COMMAND ----------

def examine_rows_column_value(df, colname, value):
    if value == None:
        tmp_df = df.where(col(colname).isNull())
    else:
        tmp_df = df[df[colname] == value]
    return tmp_df
  
def count_unique_pats(df):
    n_unique_pats = df.agg(countDistinct("PERSON_ID_DEID")).toPandas()
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

# MAGIC %md
# MAGIC ## 1) db source: 
# MAGIC load deaths database and filter registered date of death in (2020-01-01, 2020-10-31]

# COMMAND ----------

# MAGIC %md
# MAGIC ###### *global_temp.dp_deaths*

# COMMAND ----------

dth_og = spark.table("global_temp.dp_deaths") #global_temp.dp_deaths requires first running master nb
dth_og = dth_og\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID_DEID')\
  .filter(col("PERSON_ID_DEID").isNotNull())\
  .withColumn("REG_DATE_OF_DEATH", to_date("REG_DATE_OF_DEATH", "yyyyMMdd"))\
  .filter((col("REG_DATE_OF_DEATH") > to_date(lit("2020-01-01"))) & (col("REG_DATE_OF_DEATH") <= to_date(lit("2020-10-31")) ) )
count_unique_pats(dth_og)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### *dars_nic_391419_j3w9t_collab.dp_deaths_dars_nic_391419_j3w9t*

# COMMAND ----------

# dth_og = spark.table("dars_nic_391419_j3w9t_collab.dp_deaths_dars_nic_391419_j3w9t") #global_temp.dp_deaths requires first running master nb
# dth_og = dth_og\
#   .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID_DEID')\
#   .filter(col("PERSON_ID_DEID").isNotNull())\
#   .withColumn("REG_DATE_OF_DEATH", to_date("REG_DATE_OF_DEATH", "yyyyMMdd"))\
#   .filter((col("REG_DATE_OF_DEATH") > to_date(lit("2020-01-01"))) & (col("REG_DATE_OF_DEATH") <= to_date(lit("2020-10-31")) ) )
# count_unique_pats(dth_og)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2) grep COVID medcode in cause of death
# MAGIC * concatenate all "S_COD_CODE" columns and "S_UNDERLYING_COD_ICD10"
# MAGIC * list IDs with a COVID medcode in their concatenated COD
# MAGIC * remove duplicates

# COMMAND ----------

dth = dth_og
concat_columns = [f"S_COD_CODE_{i}" for i in list(range(1,16))] + ["S_UNDERLYING_COD_ICD10"]
dth = dth.withColumn("S_COD_CODE_CONCAT", f.concat_ws(",", *[f.col(x) for x in concat_columns]))

covid_icd_10_code_list = ['U071','U072','U07.2','U07.1']
dth_covid = dth.where(
    reduce(lambda a, b: a|b, (dth['S_COD_CODE_CONCAT'].like('%'+code+"%") for code in covid_icd_10_code_list))
).select(["PERSON_ID_DEID"])
dth_covid = dth_covid.dropDuplicates()
count_unique_pats(dth_covid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) inner-join with reference population table ("skinny table")
# MAGIC * restrict to included population (can implement later in table 3 nb too)

# COMMAND ----------

skinny = spark.table("dars_nic_391419_j3w9t_collab.dp_skinny_patient_01_01_2020")
skinny = skinny.withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID_DEID')
dth_covid_skinny = dth_covid.join(skinny, "PERSON_ID_DEID", "inner")
count_unique_pats(dth_covid_skinny)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### export table

# COMMAND ----------

create_table(dth_covid_skinny, table_name="dp_covid_death", database_name="dars_nic_391419_j3w9t_collab", select_sql_script=None)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## phenotypes
# MAGIC * below is code to obtain stats phenotypes for sex, age groups, ethnicity, previous MI/stroke, diabetes and obesity
# MAGIC * good sanity check with results obtained via the table 3 nb

# COMMAND ----------

# MAGIC %md 
# MAGIC ###sex

# COMMAND ----------

# dth_covid_skinny.groupBy("SEX").agg(countDistinct("PERSON_ID_DEID")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### age group

# COMMAND ----------

# dth_covid_skinny = dth_covid_skinny.withColumn("AGE_GROUP", 
#                                                when(  ((col("AGE_AT_COHORT_START") >=0) &  (col("AGE_AT_COHORT_START") < 18)), "0-17" )
#                                                .when(  ((col("AGE_AT_COHORT_START") > 17) &  (col("AGE_AT_COHORT_START") < 30)), "18-29" )
#                                                .when(  ((col("AGE_AT_COHORT_START") > 29) &  (col("AGE_AT_COHORT_START") < 50)), "30-49" )
#                                                .when(  ((col("AGE_AT_COHORT_START") > 49) &  (col("AGE_AT_COHORT_START") < 70)), "50-69" )
#                                                .when(  ((col("AGE_AT_COHORT_START") > 69) &  (col("AGE_AT_COHORT_START") < 120)), "70+" )
#                                                .otherwise("Unknown")


# )    
# display(dth_covid_skinny.select(["AGE_GROUP", "AGE_AT_COHORT_START"]))

# COMMAND ----------

# dth_covid_skinny.groupBy("AGE_GROUP").agg(countDistinct("PERSON_ID_DEID")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ethnicity

# COMMAND ----------

# dth_covid_skinny.groupBy("CATEGORISED_ETHNICITY").agg(countDistinct("PERSON_ID_DEID")).toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### previous MI/stroke

# COMMAND ----------

# mi_stroke = spark.table("dars_nic_391419_j3w9t_collab.descriptive_MI_Stroke_previous")
# #mi_stroke = mi_stroke.withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID_DEID')
# #display(mi_stroke)

# COMMAND ----------

# dth_covid_skinny_mistroke = dth_covid_skinny.join(mi_stroke, "PERSON_ID_DEID", "left")
# dth_covid_skinny_mistroke = dth_covid_skinny_mistroke.filter(col("MI_Stroke_Previous") == 1)
# count_unique_pats(dth_covid_skinny_mistroke)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### previous diabetes

# COMMAND ----------

# diab = spark.table("dars_nic_391419_j3w9t_collab.covariate_flags_diabetes")
# diab = diab.withColumnRenamed('ID', 'PERSON_ID_DEID')

# display(diab)

# COMMAND ----------

# dth_covid_skinny_diab = dth_covid_skinny.join(diab, "PERSON_ID_DEID", "left")
# dth_covid_skinny_diab = dth_covid_skinny_diab.filter(col("EVER_DIAB") == 1)
# count_unique_pats(dth_covid_skinny_diab)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### previous obesity

# COMMAND ----------

# obese = spark.table("dars_nic_391419_j3w9t_collab.covariate_flags_obesity")
# obese = obese.withColumnRenamed('ID', 'PERSON_ID_DEID')
# dth_covid_skinny_obese = dth_covid_skinny.join(obese, "PERSON_ID_DEID", "left")
# dth_covid_skinny_obese = dth_covid_skinny_obese.filter(col("EVER_OBESE") == 1)
# count_unique_pats(dth_covid_skinny_obese)


# COMMAND ----------


