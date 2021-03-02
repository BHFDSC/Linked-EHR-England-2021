# Databricks notebook source
# MAGIC %md  # 1_dp_data_wrangle_master - Master notebook to prepare the data for the DP project
# MAGIC **Description**: 
# MAGIC This notebook extracts the data from specified time point (batchId) and then applies a common cutoff date of 2020-10-31 (i.e. any records beyond this time are dropped)
# MAGIC 
# MAGIC **Project(s)**: Descriptive Paper
# MAGIC 
# MAGIC **Author(s)**: Sam Hollings
# MAGIC 
# MAGIC **Reviewer(s)**: Angela Wood
# MAGIC 
# MAGIC **Date last updated**: 2021-01-11
# MAGIC 
# MAGIC **Date last reviewed**: 2021-01-11
# MAGIC 
# MAGIC **Data last run**: 2021-01-11
# MAGIC 
# MAGIC **Data input**:
# MAGIC - N/A
# MAGIC 
# MAGIC **Data output**:
# MAGIC - N/A
# MAGIC 
# MAGIC **Software and versions** Databricks (Python and SQL)
# MAGIC 
# MAGIC **Packages and versions** Databricks runtime 6.4 ML

# COMMAND ----------

import datetime 
import pandas as pd
batch_id = 'bcaf6e02-a878-44e3-98d8-cc77f5ab2ef2'
cutoff = '2020-10-31'

copy_date = datetime.datetime.now()
prefix = 'dp_'
project_prefix = 'dp_'
database = 'dars_nic_391419_j3w9t_collab'#'dars_nic_391419_j3w9t_collab'


# COMMAND ----------

table_list = [
            {'table':'hes_apc_2021_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': False},
             #{'table':'hes_apc_1920_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': True},
             {'table':'hes_ae_2021_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ARRIVALDATE', 'ignore_cutoff': False},
             #{'table':'hes_ae_1920_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ARRIVALDATE', 'ignore_cutoff': True},
             #{'table':'hes_op_1920_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'APPTDATE', 'ignore_cutoff': True},
             {'table':'hes_op_2021_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'APPTDATE', 'ignore_cutoff': False},
             #{'table':'hes_cc_1920_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': True},
             {'table':'hes_cc_2021_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': False}, 
             {'table':'primary_care_meds_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'ProcessingPeriodDate', 'ignore_cutoff': False},
             {'table':'gdppr_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'DATE', 'ignore_cutoff': False},
             {'table':'deaths_dars_nic_391419_j3w9t','extra_columns':", to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') as REG_DATE_OF_DEATH_FORMATTED, to_date(REG_DATE, 'yyyyMMdd') as REG_DATE_FORMATTED",'date_cutoff_col':"REG_DATE_OF_DEATH_FORMATTED", 'ignore_cutoff': False},
             {'table':'sgss_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'Specimen_Date', 'ignore_cutoff': False},
             {'table':'sus_dars_nic_391419_j3w9t','extra_columns':'','date_cutoff_col':'EPISODE_START_DATE', 'ignore_cutoff': False},
             {'table':'hes_apc_all_years','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': False},
            {'table': 'hes_op_all_years','extra_columns':'','date_cutoff_col':'APPTDATE', 'ignore_cutoff': False},
           #{'table':'hes_cc_all_years','extra_columns':'','date_cutoff_col':'ADMIDATE', 'ignore_cutoff': False},
            {'table': 'hes_ae_all_years','extra_columns':'','date_cutoff_col':'ARRIVALDATE', 'ignore_cutoff': False},
]

# COMMAND ----------

pd.DataFrame(table_list)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import to_date, lit

error_list = []

for idx, row in pd.DataFrame(table_list).iterrows():
  try:
    table_name = row.table 
    cutoff_col = row.date_cutoff_col
    extra_columns_sql = row.extra_columns
    print('---- ', table_name)
    sdf_table = spark.sql(f"""SELECT '{copy_date}' as ProjectCopyDate,  
                            * {extra_columns_sql} FROM {database}.{table_name}_archive""")
    sdf_table_cutoff = sdf_table.filter(f"""{cutoff_col} <= '{cutoff}'
                                        AND BatchId = '{batch_id}'""") 


    
    sdf_table_cutoff.createOrReplaceGlobalTempView(f"{project_prefix}{table_name}")
    
    source_table = f"global_temp.{project_prefix}{table_name}"
    destination_table = f"{database}.{project_prefix}{table_name}"

    spark.sql(f"DROP TABLE IF EXISTS {destination_table}")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {destination_table} AS 
                  SELECT * FROM {source_table} WHERE FALSE""")

    spark.sql(f"""ALTER TABLE {destination_table} OWNER TO {database}""")

    spark.sql(f"""
              TRUNCATE TABLE {destination_table}
              """)

    spark.sql(f"""
             INSERT INTO {destination_table}
              SELECT * FROM {source_table}
              """)

    print(f'----> Made: {destination_table}')
    
  except Exception as error:
    print(table, ": ", error)
    error_counts.append(table)

print(error_list)
