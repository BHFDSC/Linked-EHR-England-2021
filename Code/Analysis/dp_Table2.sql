-- Databricks notebook source
-- MAGIC %md # dp_Table2
-- MAGIC  
-- MAGIC **Description** This notebook creates Table 2 for the descriptive paper.
-- MAGIC  
-- MAGIC **Project(s)** CCU005
-- MAGIC  
-- MAGIC **Author(s)** Venexia Walker
-- MAGIC  
-- MAGIC **Reviewer(s)** Angela Wood
-- MAGIC  
-- MAGIC **Date last updated** 2021-01-19, 17:15 (RD and SH)
-- MAGIC  
-- MAGIC **Date last reviewed** 2021-01-19
-- MAGIC  
-- MAGIC **Date last run** 2021-01-19, 17:15 (RD and SH)
-- MAGIC  
-- MAGIC **Data input** .dp_gdppr_,
-- MAGIC .dp_skinny_patient_01_01_2020,
-- MAGIC .dp_sus_,
-- MAGIC .dp_hes_apc_all_years,
-- MAGIC .dp_deaths_,
-- MAGIC .dp_sgss_
-- MAGIC .primary_care_meds_
-- MAGIC 
-- MAGIC **Data output** Not applicable
-- MAGIC 
-- MAGIC **Software and versions** SQL
-- MAGIC  
-- MAGIC **Packages and versions** Not applicable

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC ---- Number of unique patient records (not restricted to those alive 1st Jan 2020)
-- MAGIC 
-- MAGIC select 'GDPPR' as data, COUNT(*) as records, count(DISTINCT NHS_NUMBER_DEID) as patients FROM .dp_gdppr_ 
-- MAGIC UNION ALL
-- MAGIC SELECT 'SUS' AS data, COUNT(*) as records, count(DISTINCT NHS_NUMBER_DEID) as patients FROM .dp_sus_
-- MAGIC UNION ALL
-- MAGIC SELECT 'HES APC' AS data, COUNT(*) as records, count(DISTINCT PERSON_ID_DEID) as patients FROM .dp_hes_apc_all_years
-- MAGIC UNION ALL
-- MAGIC SELECT 'Death' AS data, COUNT(*) as records, count(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) as patients FROM .dp_deaths_
-- MAGIC UNION ALL
-- MAGIC SELECT 'SGSS' AS data, COUNT(*) as records, count(DISTINCT PERSON_ID_DEID) as patients FROM .dp_sgss_
-- MAGIC UNION ALL
-- MAGIC SELECT 'PC dispensing data' AS data, COUNT(*) as records, count(DISTINCT PERSON_ID_DEID) as patients FROM .dp_primary_care_meds_

-- COMMAND ----------

-- MAGIC %md Death data checks

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 'Death' AS data, 
-- MAGIC   COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N_patients,
-- MAGIC        COUNT(DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N_records
-- MAGIC        FROM .dp_deaths_
-- MAGIC where REG_DATE_OF_DEATH_FORMATTED > '2019-12-31'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count (DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID), S_UNDERLYING_COD_ICD10
-- MAGIC FROM .dp_deaths_
-- MAGIC where REG_DATE_OF_DEATH_FORMATTED > '2019-12-31'
-- MAGIC group by S_UNDERLYING_COD_ICD10
-- MAGIC order by count (DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) DESC

-- COMMAND ----------

-- MAGIC %md Sam addition: Natasha's (from NHS Digital GDPPR team) code for GDPPR earliest date:

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC WITH CTE  AS (
-- MAGIC SELECT * FROM .gdppr_
-- MAGIC WHERE  DATE <= REPORTING_PERIOD_END_DATE AND
-- MAGIC  year(DATE) >= YEAR_OF_BIRTH - 1  AND
-- MAGIC  DATE >= "1900-01-01" AND
-- MAGIC  RECORD_DATE >= "1948-07-05" AND
-- MAGIC  DATE IS NOT NULL AND
-- MAGIC  RECORD_DATE IS NOT NULL AND
-- MAGIC  YEAR_OF_BIRTH IS NOT NULL )
-- MAGIC  
-- MAGIC  SELECT  * FROM CTE WHERE DATE = (SELECT MIN(DATE) FROM CTE)

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC 
-- MAGIC -- Note: The following code identifies patients alive on 01-01-2020 by restricting to IDs present in the skinny patient table '.dp_skinny_patient_01_01_2020'
-- MAGIC 
-- MAGIC -- Record the dataset name, start date (null here as patient specific), number of patients alive on 01-01-2020 and number of record associated with those patients in GDPPR
-- MAGIC SELECT 'GDPPR' AS data,
-- MAGIC        null AS start_date,
-- MAGIC        COUNT(DISTINCT NHS_NUMBER_DEID) AS N_patients,
-- MAGIC        COUNT(NHS_NUMBER_DEID) AS N_records
-- MAGIC FROM .dp_gdppr_
-- MAGIC WHERE NHS_NUMBER_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020)
-- MAGIC 
-- MAGIC -- Record the dataset name, start date, number of patients alive on 01-01-2020 and number of record associated with those patients in SUS and join to existing table using 'UNION ALL'
-- MAGIC UNION ALL
-- MAGIC SELECT 'SUS' AS data,
-- MAGIC        min(EPISODE_START_DATE) AS start_date,
-- MAGIC        COUNT(DISTINCT NHS_NUMBER_DEID) AS N_patients,
-- MAGIC        COUNT(NHS_NUMBER_DEID) AS N_records
-- MAGIC FROM .dp_sus_
-- MAGIC WHERE NHS_NUMBER_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020)
-- MAGIC 
-- MAGIC -- Record the dataset name, start date, number of patients alive on 01-01-2020 and number of record associated with those patients in HES APC and join to existing table using 'UNION ALL'
-- MAGIC UNION ALL
-- MAGIC SELECT 'HES APC' AS data,
-- MAGIC        min(EPISTART) AS start_date,
-- MAGIC        COUNT(DISTINCT PERSON_ID_DEID) AS N_patients,
-- MAGIC        COUNT(PERSON_ID_DEID) AS N_records
-- MAGIC FROM .dp_hes_apc_all_years
-- MAGIC WHERE PERSON_ID_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020)
-- MAGIC 
-- MAGIC -- Record the dataset name, start date, number of patients alive on 01-01-2020 and number of record associated with those patients in the ONS death registry and join to existing table using 'UNION ALL'
-- MAGIC UNION ALL
-- MAGIC SELECT 'Death' AS data,
-- MAGIC        min(REG_DATE_OF_DEATH_FORMATTED) AS start_date,
-- MAGIC        COUNT(DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N_patients,
-- MAGIC        COUNT(DEC_CONF_NHS_NUMBER_CLEAN_DEID) AS N_records
-- MAGIC FROM .dp_deaths_
-- MAGIC WHERE DEC_CONF_NHS_NUMBER_CLEAN_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020)
-- MAGIC 
-- MAGIC -- Record the dataset name, start date, number of patients alive on 01-01-2020 and number of record associated with those patients in SGSS and join to existing table using 'UNION ALL'
-- MAGIC UNION ALL
-- MAGIC SELECT 'SGSS' AS data,
-- MAGIC        min(Specimen_Date) start_date,
-- MAGIC        COUNT(DISTINCT PERSON_ID_DEID) AS N_patients,
-- MAGIC        COUNT(PERSON_ID_DEID) AS N_records
-- MAGIC FROM .dp_sgss_
-- MAGIC WHERE PERSON_ID_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020)
-- MAGIC 
-- MAGIC -- Record the dataset name, start date, number of patients alive on 01-01-2020 and number of record associated with those patients in primary care dispensing data and join to existing table using 'UNION ALL'
-- MAGIC UNION ALL
-- MAGIC SELECT 'PC dispensing data' AS data,
-- MAGIC        min(ProcessingPeriodDate) start_date,
-- MAGIC        COUNT(DISTINCT PERSON_ID_DEID) AS N_patients,
-- MAGIC        COUNT(PERSON_ID_DEID) AS N_records
-- MAGIC FROM .dp_primary_care_meds_
-- MAGIC WHERE PERSON_ID_DEID IN (SELECT NHS_NUMBER_DEID FROM .dp_skinny_patient_01_01_2020);

-- COMMAND ----------

-- MAGIC %md getting earliest hes record in the 1997 data

-- COMMAND ----------

SELECT 'HES APC' AS data,
       min(COALESCE(EPISTART,"1800-01-01")) AS start_date--
       --COUNT(DISTINCT PERSON_ID_DEID) AS N_patients,
       --COUNT(PERSON_ID_DEID) AS N_records
FROM .hes_apc_9798_
WHERE COALESCE(EPISTART,"1800-01-01") > '1900-01-01'
