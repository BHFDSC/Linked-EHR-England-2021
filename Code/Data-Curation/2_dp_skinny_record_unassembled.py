# Databricks notebook source
# MAGIC %md # 2_dp_skinny_record_unassembled: Make the Skinny Record Unassembled Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Description** Gather together the records for each patient in primary and secondary care before they are assembled into a skinny record. This notebook is specific to the Descriptive paper work and references it's tables (prefixed with "dp_")
# MAGIC  
# MAGIC **Project(s)** Descriptive Paper
# MAGIC  
# MAGIC **Author(s)** Sam Hollings
# MAGIC  
# MAGIC **Reviewer(s)** Angela Wood
# MAGIC  
# MAGIC **Date last updated** 2021-01-11
# MAGIC  
# MAGIC **Date last reviewed** 2021-01-11
# MAGIC  
# MAGIC **Date last run** 2021-01-11
# MAGIC  
# MAGIC **Data input** [HES, GDPPR, Deaths]
# MAGIC 
# MAGIC **Data output** Table: `dars_nic_391419_j3w9t_collab.dp_patient_skinny_unassembled`
# MAGIC 
# MAGIC **Software and versions** Databricks (Python and SQL)
# MAGIC  
# MAGIC **Packages and versions** Databricks runtime 6.4 ML

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will make a single record for each patient with the core facts about that patient, reconciled across the main datasets (primary and secondary care)
# MAGIC 
# MAGIC |Column | Content|
# MAGIC |----------------|--------------------|
# MAGIC |NHS_NUMBER_DEID | Patient NHS Number |
# MAGIC |ETHNIC | Patient Ethnicity |
# MAGIC |SEX | Patient Sex |
# MAGIC |DATE_OF_BIRTH | Patient Date of Birth (month level) |
# MAGIC |DATE_OF_DEATH | Patient Date of Death (month level) |
# MAGIC |record_id | The id of the record from which the data was drawn |
# MAGIC |dataset | The dataset from which the record comes from |
# MAGIC |primary | Whether the record refers to primary of secondary care |

# COMMAND ----------

# MAGIC %md need the ability to select a specific batch to work from

# COMMAND ----------

batch_id = 'bcaf6e02-a878-44e3-98d8-cc77f5ab2ef2'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug
# MAGIC - Angela says to remove OP and AE for now - they're not well used and won't really add a lot of patients
# MAGIC - Angela says consider which HES years are going weird with an explosion of patients
# MAGIC - Angela also says find out when the death dataset starts - if we are linking to hes before this, then maybe some deaths won't be included and people won't be marked as dead?(2008 rings a bell....)

# COMMAND ----------

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md ### Get the secondary care data for each patient
# MAGIC First pull all the patient facts from HES

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW dp_all_hes_apc AS
 SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      to_date(MYDOB,'MMyyyy') as DATE_OF_BIRTH , 
      NULL as DATE_OF_DEATH, 
      EPISTART as RECORD_DATE, 
      epikey as record_id,
      "hes_apc" as dataset,
      0 as primary,
      FYEAR
  FROM dars_nic_391419_j3w9t_collab.dp_hes_apc_all_years"""
)

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW dp_all_hes_ae AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      date_format(date_trunc("MM", date_add(ARRIVALDATE, -ARRIVALAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH, 
      ARRIVALDATE as RECORD_DATE, 
      COALESCE(epikey, aekey) as record_id,
      "hes_ae" as dataset,
      0 as primary,
      FYEAR
  FROM dars_nic_391419_j3w9t_collab.dp_hes_ae_all_years"""
         )

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW dp_all_hes_op AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX,
      date_format(date_trunc("MM", date_add(APPTDATE, -APPTAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH,
      APPTDATE  as RECORD_DATE,
      ATTENDKEY as record_id,
      'hes_op' as dataset,
      0 as primary,
      FYEAR
  FROM dars_nic_391419_j3w9t_collab.dp_hes_op_all_years"""
         )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_all_hes as
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.dp_all_hes_apc
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.dp_all_hes_ae
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.dp_all_hes_op

# COMMAND ----------

# MAGIC %md ## Primary care for each patient
# MAGIC Get the patients in the standard template from GDPPR
# MAGIC 
# MAGIC These values are standard for a patient across the system, so its hard to assign a date, so Natasha from primary care told me they use `REPORTING_PERIOD_END_DATE` as the date for these patient features

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_gdppr_patients AS
# MAGIC     SELECT NHS_NUMBER_DEID,
# MAGIC           ETHNIC,
# MAGIC           SEX,
# MAGIC           DATE_OF_BIRTH,
# MAGIC           DATE_OF_DEATH,
# MAGIC           RECORD_DATE,
# MAGIC           record_id,
# MAGIC           dataset,
# MAGIC           primary
# MAGIC     FROM (
# MAGIC                 SELECT NHS_NUMBER_DEID, 
# MAGIC                       gdppr.ETHNIC, 
# MAGIC                       gdppr.SEX,
# MAGIC                       to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
# MAGIC                       to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
# MAGIC                       REPORTING_PERIOD_END_DATE as RECORD_DATE, -- I got this off Natasha from Primary Care
# MAGIC                       NULL as record_id,
# MAGIC                       'GDPPR' as dataset,
# MAGIC                       1 as primary
# MAGIC                 FROM dars_nic_391419_j3w9t_collab.dp_gdppr_dars_nic_391419_j3w9t as gdppr 
# MAGIC         )

# COMMAND ----------

# MAGIC %md GDPPR can also store the patient ethnicity in the `CODE` column as a SNOMED code, hence we need to bring this in as another record for the patient (but with null for the other features as they come from the generic record above)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_gdppr_patients_SNOMED AS
# MAGIC     SELECT NHS_NUMBER_DEID,
# MAGIC           ETHNIC,
# MAGIC           SEX,
# MAGIC           DATE_OF_BIRTH,
# MAGIC           DATE_OF_DEATH,
# MAGIC           RECORD_DATE,
# MAGIC           record_id,
# MAGIC           dataset,
# MAGIC           primary
# MAGIC     FROM (
# MAGIC                 SELECT NHS_NUMBER_DEID, 
# MAGIC                       eth.PrimaryCode as ETHNIC, 
# MAGIC                       gdppr.SEX,
# MAGIC                       to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
# MAGIC                       to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
# MAGIC                       DATE as RECORD_DATE,
# MAGIC                       NULL as record_id,
# MAGIC                       'GDPPR_snomed' as dataset,
# MAGIC                       1 as primary
# MAGIC                 FROM dars_nic_391419_j3w9t_collab.dp_gdppr_dars_nic_391419_j3w9t as gdppr
# MAGIC                 INNER JOIN dss_corporate.gdppr_ethnicity_mappings eth on gdppr.CODE = eth.ConceptId             
# MAGIC         )

# COMMAND ----------

# MAGIC %md ### Single death per patient
# MAGIC In the deaths table (Civil registration deaths), some unfortunate people are down as dying twice. Let's take the most recent death date. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_single_patient_death AS
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC   (SELECT * , row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
# MAGIC                                       ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc) as death_rank
# MAGIC     FROM dars_nic_391419_j3w9t_collab.dp_deaths_dars_nic_391419_j3w9t
# MAGIC     ) cte
# MAGIC WHERE death_rank = 1
# MAGIC AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
# MAGIC and REG_DATE_OF_DEATH_formatted > '1900-01-01'
# MAGIC AND REG_DATE_OF_DEATH_formatted <= current_date()

# COMMAND ----------

# MAGIC %md ## Combine Primary and Secondary Care along with Deaths data
# MAGIC Flag some values as NULLs:
# MAGIC - DATE_OF_DEATH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - DATE_OF_BIRTH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - SEX flag the following as NULL: 'NULL', empty string, "9", "0" (9 and 0 are coded nulls, like unknown or not specified)
# MAGIC - ETHNIC flag the following as MULL: 'NULL', empty string, "9", "99", "X", "Z" - various types of coded nulls (unknown etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_patient_skinny_hes_gdppr AS
# MAGIC SELECT *,      
# MAGIC       CASE WHEN ETHNIC IS NULL or TRIM(ETHNIC) IN ("","9", "99", "X" , "Z") THEN 1 ELSE 0 END as ethnic_null,
# MAGIC       CASE WHEN SEX IS NULL or TRIM(SEX) IN ("", "9", "0" ) THEN 1 ELSE 0 END as sex_null,
# MAGIC       CASE WHEN DATE_OF_BIRTH IS NULL OR TRIM(DATE_OF_BIRTH) = "" OR DATE_OF_BIRTH < '1900-01-01' or DATE_OF_BIRTH > current_date() OR DATE_OF_BIRTH > RECORD_DATE THEN 1 ELSE 0 END as date_of_birth_null,
# MAGIC       CASE WHEN DATE_OF_DEATH IS NULL OR TRIM(DATE_OF_DEATH) = "" OR DATE_OF_DEATH < '1900-01-01' OR DATE_OF_DEATH > current_date() OR DATE_OF_DEATH > RECORD_DATE THEN 1 ELSE 0 END as date_of_death_null,
# MAGIC       CASE WHEN dataset = 'death' THEN 1 ELSE 0 END as death_table
# MAGIC FROM (
# MAGIC       SELECT  NHS_NUMBER_DEID,
# MAGIC               ETHNIC,
# MAGIC               SEX,
# MAGIC               DATE_OF_BIRTH,
# MAGIC               DATE_OF_DEATH,
# MAGIC               RECORD_DATE,
# MAGIC               record_id,
# MAGIC               dataset,
# MAGIC               primary,
# MAGIC               care_domain        
# MAGIC       FROM (
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'primary' as care_domain
# MAGIC               FROM global_temp.dp_gdppr_patients 
# MAGIC             UNION ALL
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'primary_SNOMED' as care_domain
# MAGIC               FROM global_temp.dp_gdppr_patients_SNOMED
# MAGIC             UNION ALL
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'secondary' as care_domain
# MAGIC               FROM global_temp.dp_all_hes
# MAGIC             UNION ALL
# MAGIC             SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID,
# MAGIC                 Null as ETHNIC,
# MAGIC                 Null as SEX,
# MAGIC                 Null as DATE_OF_BIRTH,
# MAGIC                 REG_DATE_OF_DEATH_formatted as DATE_OF_DEATH,
# MAGIC                 REG_DATE_formatted as RECORD_DATE,
# MAGIC                 Null as record_id,
# MAGIC                 'death' as dataset,
# MAGIC                 0 as primary, 'death' as care_domain
# MAGIC               FROM global_temp.dp_single_patient_death
# MAGIC           ) all_patients 
# MAGIC           --LEFT JOIN dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t death on all_patients.NHS_NUMBER_DEID = death.DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC     )

# COMMAND ----------

drop_table(table_name='dp_patient_skinny_unassembled')

# COMMAND ----------

create_table(table_name='dp_patient_skinny_unassembled', select_sql_script='SELECT * FROM global_temp.dp_patient_skinny_hes_gdppr')

# COMMAND ----------

# MAGIC %md ## Presence table - which datasets each patient was in

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_patient_dataset_presence_lookup AS
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC       COALESCE(deaths, 0) as deaths,
# MAGIC       COALESCE(sgss, 0) as sgss,
# MAGIC       COALESCE(gdppr,0) as gdppr,
# MAGIC       COALESCE(hes_apc, 0) as hes_apc,
# MAGIC       COALESCE(hes_op, 0) as hes_op,
# MAGIC       COALESCE(hes_ae, 0) as hes_ae,
# MAGIC       CASE WHEN hes_ae = 1 or hes_apc=1 or hes_op = 1 THEN 1 ELSE 0 END as hes
# MAGIC FROM (
# MAGIC SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID, "deaths" as data_table, 1 as presence FROM dars_nic_391419_j3w9t_collab.dp_deaths_dars_nic_391419_j3w9t
# MAGIC union all
# MAGIC SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, "sgss" as data_table, 1 as presence FROM dars_nic_391419_j3w9t_collab.dp_sgss_dars_nic_391419_j3w9t
# MAGIC union all
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID, "gdppr" as data_table, 1 as presence FROM global_temp.dp_gdppr_patients
# MAGIC union all
# MAGIC -- SELECT DISTINCT NHS_NUMBER_DEID, "hes" as data_table, 1 as presence FROM global_temp.all_hes
# MAGIC -- union all
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID, "hes_apc" as data_table, 1 as presence FROM global_temp.dp_all_hes_apc
# MAGIC union all
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID, "hes_ae" as data_table, 1 as presence FROM global_temp.dp_all_hes_ae
# MAGIC union all
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID, "hes_op" as data_table, 1 as presence FROM global_temp.dp_all_hes_op
# MAGIC )
# MAGIC PIVOT (MAX(presence) FOR data_table in ("deaths", "sgss", "gdppr", "hes_apc", "hes_op", "hes_ae"))

# COMMAND ----------

drop_table(table_name='dp_patient_dataset_presence_lookup')
create_table(table_name='dp_patient_dataset_presence_lookup')
