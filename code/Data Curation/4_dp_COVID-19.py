# Databricks notebook source
# MAGIC %md
# MAGIC  # 4_dp_COVID-19: Makes COVID-19 patient covariate lookups 
# MAGIC  
# MAGIC **Description** This notebooks make a table `covid19_test_all3` containing the patients which have had COVID-19. This has been derived from:
# MAGIC - GDPPR (list of accepted confirmed COVID19 (lab and clinically confirmed) SNOMED codes, see below)
# MAGIC - SGSS (all positive tests)
# MAGIC - HES_APC (ICD10 codes U072 and U071 in any diagnostic position).
# MAGIC 
# MAGIC **Project(s)** Descriptive Manuscript
# MAGIC  
# MAGIC **Author(s)** Rachel Denholm
# MAGIC  
# MAGIC **Reviewer(s)** Sam Hollings
# MAGIC  
# MAGIC **Date last updated** 2021-01-11
# MAGIC  
# MAGIC **Date last reviewed** 2020-12-07
# MAGIC  
# MAGIC **Date last run** 2020-11-30
# MAGIC  
# MAGIC **Data input** HES_APC, GDPPR, SGSS
# MAGIC 
# MAGIC **Data output** table: `covid19_test_all`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will create a single table that contains all COVID-19 sources
# MAGIC 
# MAGIC |Column | Content|
# MAGIC |----------------|--------------------|
# MAGIC |NHS_NUMBER_DEID | Patient NHS Number |
# MAGIC |record_date | Date of event: date in GDPPR, speciment date SGSS, epistart HES |
# MAGIC |Covid-19_status | Cateogrical: Positive PCR test; Confirmed_COVID19; Suspected_COVID19; Lab confirmed incidence; Lab confirmed historic; Lab confirmed unclear; Clinically confirmed |
# MAGIC |Source | Source from which the data was drawn: SGSS; HES APC; Primary care |
# MAGIC |Code | Type of code: ICD10; SNOMED |
# MAGIC |Clinical_Code | Reported clinical code |
# MAGIC |description | Description of the clinical code |
# MAGIC |Pillar | Test conducted as part of Pillar 1 or 2 |
# MAGIC |Status | Covid-19 aligned status: Confirmed (either lab or clinically); Suspected |

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

# MAGIC %sql 
# MAGIC --- SGSS table
# MAGIC -- all records as every record is a "positive test"
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_covid19_test_sgss  AS
# MAGIC 
# MAGIC SELECT person_id_deid as nhs_number_deid, 
# MAGIC specimen_date as record_date, 
# MAGIC "Positive PCR test" as Covid19_status, 
# MAGIC "SGSS" as Source, 
# MAGIC "" as Clinical_code, 
# MAGIC "" as Code, 
# MAGIC "" as description,
# MAGIC CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as pillar
# MAGIC from global_temp.dp_sgss

# COMMAND ----------

# %sql
# ALTER TABLE dars_nic_391419_j3w9t_collab.covid19_test_sgss  OWNER TO dars_nic_391419_j3w9t_collab

# COMMAND ----------

# MAGIC %sql 
# MAGIC -----HES data
# MAGIC -- checks all diag columns for U07.1 and U07.1 (also U071 and U072)
# MAGIC -- flags patients with Covid Status as
# MAGIC -- - Confirmed_COVID19 for U07.1/U071
# MAGIC -- - Suspected_COVID19 for U07.2/U072
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_covid19_test_hes as
# MAGIC 
# MAGIC with cte_hes as (
# MAGIC SELECT PERSON_ID_DEID as nhs_number_deid, EPISTART as record_date, "HES APC" as Source, "ICD10" as Code, "" as description,
# MAGIC DIAG_4_01, DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
# MAGIC DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, 
# MAGIC DIAG_4_19, DIAG_4_20, 
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_01,'[X]$','') , 4 ) AS DIAG_4_01_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_02,'[X]$','') , 4 ) AS DIAG_4_02_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_03,'[X]$','') , 4 ) AS DIAG_4_03_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_04,'[X]$','') , 4 ) AS DIAG_4_04_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_05,'[X]$','') , 4 ) AS DIAG_4_05_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_06,'[X]$','') , 4 ) AS DIAG_4_06_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_07,'[X]$','') , 4 ) AS DIAG_4_07_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_08,'[X]$','') , 4 ) AS DIAG_4_08_trunc, 
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_09,'[X]$','') , 4 ) AS DIAG_4_09_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_10,'[X]$','') , 4 ) AS DIAG_4_10_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_11,'[X]$','') , 4 ) AS DIAG_4_11_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_12,'[X]$','') , 4 ) AS DIAG_4_12_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_13,'[X]$','') , 4 ) AS DIAG_4_13_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_14,'[X]$','') , 4 ) AS DIAG_4_14_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_15,'[X]$','') , 4 ) AS DIAG_4_15_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_16,'[X]$','') , 4 ) AS DIAG_4_16_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_17,'[X]$','') , 4 ) AS DIAG_4_17_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_18,'[X]$','') , 4 ) AS DIAG_4_18_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_19,'[X]$','') , 4 ) AS DIAG_4_19_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc,
# MAGIC  SUSRECID
# MAGIC FROM global_temp.dp_hes_apc_all
# MAGIC )
# MAGIC 
# MAGIC SELECT nhs_number_deid, record_date, Source, Code, description,
# MAGIC (case when DIAG_4_01_trunc = 'U07.1' OR DIAG_4_01_trunc = 'U071' OR DIAG_4_02_trunc = 'U07.1' OR DIAG_4_02_trunc = 'U071'
# MAGIC OR DIAG_4_03_trunc = 'U07.1' OR DIAG_4_03_trunc = 'U071' OR DIAG_4_04_trunc = 'U07.1' OR DIAG_4_04_trunc = 'U071' 
# MAGIC OR DIAG_4_05_trunc = 'U07.1' OR DIAG_4_05_trunc = 'U071' OR DIAG_4_06_trunc = 'U07.1' OR DIAG_4_06_trunc = 'U071' 
# MAGIC OR DIAG_4_07_trunc = 'U07.1' OR DIAG_4_07_trunc = 'U071' OR DIAG_4_08_trunc = 'U07.1' OR DIAG_4_08_trunc = 'U071' 
# MAGIC OR DIAG_4_09_trunc = 'U07.1' OR DIAG_4_09_trunc = 'U071' OR DIAG_4_10_trunc = 'U07.1' OR DIAG_4_10_trunc = 'U071' 
# MAGIC OR DIAG_4_11_trunc = 'U07.1' OR DIAG_4_11_trunc = 'U071' OR DIAG_4_12_trunc = 'U07.1' OR DIAG_4_12_trunc = 'U071' 
# MAGIC OR DIAG_4_13_trunc = 'U07.1' OR DIAG_4_13_trunc = 'U071' OR DIAG_4_14_trunc = 'U07.1' OR DIAG_4_14_trunc = 'U071' 
# MAGIC OR DIAG_4_15_trunc = 'U07.1' OR DIAG_4_15_trunc = 'U071' OR DIAG_4_16_trunc = 'U07.1' OR DIAG_4_16_trunc = 'U071'  
# MAGIC OR DIAG_4_17_trunc = 'U07.1' OR DIAG_4_17_trunc = 'U071' OR DIAG_4_18_trunc = 'U07.1' OR DIAG_4_18_trunc = 'U071'
# MAGIC OR DIAG_4_19_trunc = 'U07.1' OR DIAG_4_19_trunc = 'U071' OR DIAG_4_20_trunc = 'U07.1' OR DIAG_4_20_trunc = 'U071' THEN 'Confirmed_COVID19'
# MAGIC when DIAG_4_01_trunc = 'U07.2' OR DIAG_4_01_trunc = 'U072' OR DIAG_4_02_trunc = 'U07.2' OR DIAG_4_02_trunc = 'U072'
# MAGIC OR DIAG_4_03_trunc = 'U07.2' OR DIAG_4_03_trunc = 'U072' OR DIAG_4_04_trunc = 'U07.2' OR DIAG_4_04_trunc = 'U072' 
# MAGIC OR DIAG_4_05_trunc = 'U07.2' OR DIAG_4_05_trunc = 'U072' OR DIAG_4_06_trunc = 'U07.2' OR DIAG_4_06_trunc = 'U072' 
# MAGIC OR DIAG_4_07_trunc = 'U07.2' OR DIAG_4_07_trunc = 'U072' OR DIAG_4_08_trunc = 'U07.2' OR DIAG_4_08_trunc = 'U072' 
# MAGIC OR DIAG_4_09_trunc = 'U07.2' OR DIAG_4_09_trunc = 'U072' OR DIAG_4_10_trunc = 'U07.2' OR DIAG_4_10_trunc = 'U072' 
# MAGIC OR DIAG_4_11_trunc = 'U07.2' OR DIAG_4_11_trunc = 'U072' OR DIAG_4_12_trunc = 'U07.2' OR DIAG_4_12_trunc = 'U072' 
# MAGIC OR DIAG_4_13_trunc = 'U07.2' OR DIAG_4_13_trunc = 'U072' OR DIAG_4_14_trunc = 'U07.2' OR DIAG_4_14_trunc = 'U072' 
# MAGIC OR DIAG_4_15_trunc = 'U07.2' OR DIAG_4_15_trunc = 'U072' OR DIAG_4_16_trunc = 'U07.2' OR DIAG_4_16_trunc = 'U072'  
# MAGIC OR DIAG_4_17_trunc = 'U07.2' OR DIAG_4_17_trunc = 'U072' OR DIAG_4_18_trunc = 'U07.2' OR DIAG_4_18_trunc = 'U072'
# MAGIC OR DIAG_4_19_trunc = 'U07.2' OR DIAG_4_19_trunc = 'U072' OR DIAG_4_20_trunc = 'U07.2' OR DIAG_4_20_trunc = 'U072' THEN 'Suspected_COVID19'
# MAGIC Else '0' End) as Covid19_status,
# MAGIC (case when DIAG_4_01_trunc = 'U07.1' OR DIAG_4_01_trunc = 'U071' OR DIAG_4_02_trunc = 'U07.1' OR DIAG_4_02_trunc = 'U071'
# MAGIC OR DIAG_4_03_trunc = 'U07.1' OR DIAG_4_03_trunc = 'U071' OR DIAG_4_04_trunc = 'U07.1' OR DIAG_4_04_trunc = 'U071' 
# MAGIC OR DIAG_4_05_trunc = 'U07.1' OR DIAG_4_05_trunc = 'U071' OR DIAG_4_06_trunc = 'U07.1' OR DIAG_4_06_trunc = 'U071' 
# MAGIC OR DIAG_4_07_trunc = 'U07.1' OR DIAG_4_07_trunc = 'U071' OR DIAG_4_08_trunc = 'U07.1' OR DIAG_4_08_trunc = 'U071' 
# MAGIC OR DIAG_4_09_trunc = 'U07.1' OR DIAG_4_09_trunc = 'U071' OR DIAG_4_10_trunc = 'U07.1' OR DIAG_4_10_trunc = 'U071' 
# MAGIC OR DIAG_4_11_trunc = 'U07.1' OR DIAG_4_11_trunc = 'U071' OR DIAG_4_12_trunc = 'U07.1' OR DIAG_4_12_trunc = 'U071' 
# MAGIC OR DIAG_4_13_trunc = 'U07.1' OR DIAG_4_13_trunc = 'U071' OR DIAG_4_14_trunc = 'U07.1' OR DIAG_4_14_trunc = 'U071' 
# MAGIC OR DIAG_4_15_trunc = 'U07.1' OR DIAG_4_15_trunc = 'U071' OR DIAG_4_16_trunc = 'U07.1' OR DIAG_4_16_trunc = 'U071'  
# MAGIC OR DIAG_4_17_trunc = 'U07.1' OR DIAG_4_17_trunc = 'U071' OR DIAG_4_18_trunc = 'U07.1' OR DIAG_4_18_trunc = 'U071'
# MAGIC OR DIAG_4_19_trunc = 'U07.1' OR DIAG_4_19_trunc = 'U071' OR DIAG_4_20_trunc = 'U07.1' OR DIAG_4_20_trunc = 'U071' THEN 'U07.1'
# MAGIC when DIAG_4_01_trunc = 'U07.2' OR DIAG_4_01_trunc = 'U072' OR DIAG_4_02_trunc = 'U07.2' OR DIAG_4_02_trunc = 'U072'
# MAGIC OR DIAG_4_03_trunc = 'U07.2' OR DIAG_4_03_trunc = 'U072' OR DIAG_4_04_trunc = 'U07.2' OR DIAG_4_04_trunc = 'U072' 
# MAGIC OR DIAG_4_05_trunc = 'U07.2' OR DIAG_4_05_trunc = 'U072' OR DIAG_4_06_trunc = 'U07.2' OR DIAG_4_06_trunc = 'U072' 
# MAGIC OR DIAG_4_07_trunc = 'U07.2' OR DIAG_4_07_trunc = 'U072' OR DIAG_4_08_trunc = 'U07.2' OR DIAG_4_08_trunc = 'U072' 
# MAGIC OR DIAG_4_09_trunc = 'U07.2' OR DIAG_4_09_trunc = 'U072' OR DIAG_4_10_trunc = 'U07.2' OR DIAG_4_10_trunc = 'U072' 
# MAGIC OR DIAG_4_11_trunc = 'U07.2' OR DIAG_4_11_trunc = 'U072' OR DIAG_4_12_trunc = 'U07.2' OR DIAG_4_12_trunc = 'U072' 
# MAGIC OR DIAG_4_13_trunc = 'U07.2' OR DIAG_4_13_trunc = 'U072' OR DIAG_4_14_trunc = 'U07.2' OR DIAG_4_14_trunc = 'U072' 
# MAGIC OR DIAG_4_15_trunc = 'U07.2' OR DIAG_4_15_trunc = 'U072' OR DIAG_4_16_trunc = 'U07.2' OR DIAG_4_16_trunc = 'U072'  
# MAGIC OR DIAG_4_17_trunc = 'U07.2' OR DIAG_4_17_trunc = 'U072' OR DIAG_4_18_trunc = 'U07.2' OR DIAG_4_18_trunc = 'U072'
# MAGIC OR DIAG_4_19_trunc = 'U07.2' OR DIAG_4_19_trunc = 'U072' OR DIAG_4_20_trunc = 'U07.2' OR DIAG_4_20_trunc = 'U072' THEN 'U07.2'
# MAGIC Else '0' End) as Clinical_code, SUSRECID
# MAGIC From cte_hes

# COMMAND ----------

# %sql
# ALTER TABLE dars_nic_391419_j3w9t_collab.covid19_test_hes  OWNER TO dars_nic_391419_j3w9t_collab

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Covid-1 Status groups:
# MAGIC -- - Lab confirmed incidence
# MAGIC -- - Lab confirmed historic
# MAGIC -- - Lab confirmed unclear
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_covid19_cats_SNOMED AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# MAGIC ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# MAGIC ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# MAGIC ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# MAGIC ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# MAGIC ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# MAGIC ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# MAGIC ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# MAGIC ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# MAGIC ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed unclear"),
# MAGIC ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# MAGIC ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# MAGIC ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
# MAGIC 
# MAGIC AS tab(conceptID, term, sensitive_status, include_binary, Covid19_status);
# MAGIC 
# MAGIC -----------------------
# MAGIC 
# MAGIC SELECT * FROM global_temp.dp_covid19_cats_SNOMED 

# COMMAND ----------

# MAGIC %sql 
# MAGIC ----- GDPPR data
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_covid19_test_gdppr as
# MAGIC 
# MAGIC with cte_gdppr as (
# MAGIC SELECT tab2.NHS_NUMBER_DEID, tab2.DATE, tab2.code, tab1.conceptid, tab1.term, tab1.Covid19_status
# MAGIC FROM  global_temp.dp_covid19_cats_SNOMED tab1
# MAGIC inner join global_temp.dp_gdppr tab2 on tab1.conceptid = tab2.code
# MAGIC )
# MAGIC 
# MAGIC SELECT NHS_NUMBER_DEID as nhs_number_deid, DATE as record_date, Covid19_status,  "GDPPR" as Source, conceptid as Clinical_code, "SNOMED" as Code, term as description
# MAGIC from cte_gdppr

# COMMAND ----------

# %sql
# ALTER TABLE dars_nic_391419_j3w9t_collab.covid19_test_gdppr  OWNER TO dars_nic_391419_j3w9t_collab

# COMMAND ----------

# MAGIC %sql 
# MAGIC -----All datasets together
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dp_covid19_test_all as
# MAGIC 
# MAGIC SELECT nhs_number_deid, record_date, Source, Covid19_status, Code, description, Clinical_code, pillar
# MAGIC FROM global_temp.dp_covid19_test_sgss
# MAGIC UNION ALL
# MAGIC SELECT nhs_number_deid, record_date, Source, Covid19_status, Code, description, Clinical_code, Null as pillar
# MAGIC FROM global_temp.dp_covid19_test_hes
# MAGIC WHERE Clinical_code = 'U07.1' OR Clinical_code = 'U07.2'
# MAGIC UNION ALL
# MAGIC SELECT nhs_number_deid, record_date, Source, Covid19_status, Code, description, Clinical_code, Null as pillar
# MAGIC FROM global_temp.dp_covid19_test_gdppr;

# COMMAND ----------

# %sql
# ALTER TABLE dars_nic_391419_j3w9t_collab.covid19_test_all OWNER TO dars_nic_391419_j3w9t_collab

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.dp_covid19_test_all3

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Status code, aligning all data sources (COVID-19 confirmed or suspected)
# MAGIC 
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.dp_covid19_test_all3 as
# MAGIC 
# MAGIC SELECT *,
# MAGIC (case when Covid19_Status = 'Positive PCR test' OR Covid19_Status = 'Confirmed_COVID19' OR Covid19_Status = 'Lab confirmed incidence' OR Covid19_Status = 'Lab confirmed historic'
# MAGIC OR Covid19_Status = 'Lab confirmed unclear' OR Covid19_Status = 'Clinically confirmed' THEN 'Confirmed COVID19'
# MAGIC when Covid19_Status = 'Suspected_COVID19' THEN 'Suspected COVID19'
# MAGIC Else '0' End) as Status
# MAGIC from global_temp.dp_covid19_test_all 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dars_nic_391419_j3w9t_collab.dp_covid19_test_all3 OWNER TO dars_nic_391419_j3w9t_collab
