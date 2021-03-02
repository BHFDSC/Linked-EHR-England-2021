# Databricks notebook source
# MAGIC %md
# MAGIC  # dp_Fig2_venn: Descriptive Paper, Fig 2 - Venn Diagram
# MAGIC  
# MAGIC **Description** Outputs for the Venn diagram in the desctipive paper
# MAGIC 
# MAGIC **Project(s)** Descriptive Manuscript
# MAGIC  
# MAGIC **Author(s)** Sam Hollings
# MAGIC  
# MAGIC **Reviewer(s)** Angela Wood
# MAGIC  
# MAGIC **Date last updated** 2020-12-21
# MAGIC  
# MAGIC **Date last reviewed** 2020-12-21
# MAGIC  
# MAGIC **Date last run** 2020-12-21
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output** None
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC  
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %md Figure 2 will contain summaries of the number of patients found in the different datasets alive after 1st Jan 2020 with various predicates, such as "with reported ethnicity".
# MAGIC 
# MAGIC It has 4 subfigures:
# MAGIC 
# MAGIC a) Number of individuals (alive on 1st Jan 2020) with a reported ethnicity (nb: use records from any time period to define reported ethnicity)
# MAGIC - Primary Care
# MAGIC - Hospital Admissions
# MAGIC 
# MAGIC b) Number of individuals (alive...) with a reported CVD event (or stroke & MI separately) in period 1st Jan 2020 to latest available date
# MAGIC - Primary Care
# MAGIC - Hospial Admissions
# MAGIC - Death Registry
# MAGIC 
# MAGIC c) Number of individuals (alive...) with a reported suspected COVID-19 +ve test in period 1st Jan 2020 to latest available date. (this is split into two further sub figures to avoid a 4 way Venn)
# MAGIC - c1)
# MAGIC   - Primary Care
# MAGIC   - hospital admissions
# MAGIC   - Death registry
# MAGIC - c2)
# MAGIC   - Primary Care or Hospital Admissions or Death Registry
# MAGIC   - SGSS

# COMMAND ----------

# MAGIC %md ## Get the data and cut to the just those alive after 1st Jan 2020
# MAGIC This uses the patients found in the reconciled "skinny record" - see the notebook **sam_h_skinny_record** in the **`descriptive_paper/data`** folder.

# COMMAND ----------

# MAGIC %md Cut down to the just the patients who are alive after 1st Jan 2020

# COMMAND ----------

# MAGIC %md
# MAGIC - in GDPPR OR in  HES_APC (only HES years 2000 onwards)
# MAGIC - Alive as of 1st Jan 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW alive_patients_2020 AS
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM global_temp.dp_skinny

# COMMAND ----------

# MAGIC %md distinct patients alive with or without ethnicity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Primary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020 FROM global_temp.alive_patients_2020 WHERE primary = 1
# MAGIC Union All
# MAGIC SELECT "Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020 FROM global_temp.alive_patients_2020 WHERE hes_apc = 1
# MAGIC UNION ALL
# MAGIC SELECT "Primary AND NOT Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020 FROM global_temp.alive_patients_2020 WHERE primary = 1 and hes_apc = 0
# MAGIC UNION ALL
# MAGIC SELECT "NOT Primary AND Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020 FROM global_temp.alive_patients_2020 WHERE primary = 0 and hes_apc = 1
# MAGIC UNION ALL
# MAGIC SELECT "Primary AND Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020 FROM global_temp.alive_patients_2020 WHERE primary = 1 and hes_apc = 1

# COMMAND ----------

# MAGIC %md distinct patients alive with ethnicity:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(CASE WHEN eth.primary = 1 and eth.secondary = 0 then 1 else 0 END) as Primary_not_secondary,
# MAGIC       SUM(CASE WHEN eth.primary = 1 and eth.secondary = 1 then 1 else 0 END) as Both,
# MAGIC       SUM(CASE WHEN eth.primary = 0 and eth.secondary = 1 then 1 else 0 END) as Secondary_not_primary
# MAGIC FROM ( 
# MAGIC       SELECT NHS_NUMBER_DEID, MAX(primary) as primary, MAX(secondary) as secondary
# MAGIC       FROM (
# MAGIC             SELECT eth.NHS_NUMBER_DEID, 
# MAGIC                   eth.ETHNIC,
# MAGIC                   CASE WHEN eth.care_domain = 'primary_SNOMED' THEN 1 ELSE 0 END as primary,
# MAGIC                   CASE WHEN eth.care_domain = 'secondary' THEN 1 ELSE 0 END as secondary
# MAGIC                   FROM dars_nic_391419_j3w9t_collab.dp_patient_fields_ranked_pre_cutoff eth
# MAGIC             WHERE eth.ethnic_null = 0 AND NHS_NUMBER_DEID IN (SELECT NHS_NUMBER_DEID FROM global_temp.alive_patients_2020)
# MAGIC             )
# MAGIC       GROUP BY NHS_NUMBER_DEID
# MAGIC       ) eth

# COMMAND ----------

# MAGIC %md ## Fig 2b First stroke

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW patient_stroke_dataset_flags AS
# MAGIC 
# MAGIC WITH CTE as (SELECT NHS_NUMBER_DEID, Source FROM dars_nic_391419_j3w9t_collab.dp_firststroke_event_table )
# MAGIC 
# MAGIC SELECT pat.*, 
# MAGIC   CASE WHEN hes_apc.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as stroke_hes_apc,
# MAGIC   CASE WHEN gdppr.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as stroke_gdppr,
# MAGIC   CASE WHEN death.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as stroke_death
# MAGIC FROM global_temp.alive_patients_2020 pat
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'Death') death ON pat.NHS_NUMBER_DEID = death.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'HES-APC') hes_apc ON pat.NHS_NUMBER_DEID = hes_apc.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'GDPPR') gdppr ON pat.NHS_NUMBER_DEID = gdppr.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.patient_stroke_dataset_flags
# MAGIC )
# MAGIC   SELECT "Primary & NOT HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 1 and stroke_hes_apc = 0 and stroke_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 1 and stroke_hes_apc = 1 and stroke_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 0 and stroke_hes_apc = 1 and stroke_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 0 and stroke_hes_apc = 1 and stroke_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 0 and stroke_hes_apc = 0 and stroke_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 1 and stroke_hes_apc = 0 and stroke_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 1 and stroke_hes_apc = 1 and stroke_death = 1
# MAGIC Union All
# MAGIC   SELECT "Primary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_gdppr = 1
# MAGIC union all
# MAGIC   SELECT "Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_hes_apc = 1
# MAGIC Union All
# MAGIC   SELECT "Death_table" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE stroke_death = 1

# COMMAND ----------

# MAGIC %md ## Fig 2b First MI

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW patient_mi_dataset_flags AS
# MAGIC 
# MAGIC WITH CTE as (SELECT NHS_NUMBER_DEID, Source FROM dars_nic_391419_j3w9t_collab.dp_firstmi_event_table )
# MAGIC 
# MAGIC SELECT pat.*, 
# MAGIC   CASE WHEN hes_apc.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as mi_hes_apc,
# MAGIC   CASE WHEN gdppr.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as mi_gdppr,
# MAGIC   CASE WHEN death.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as mi_death
# MAGIC FROM global_temp.alive_patients_2020 pat
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'Death') death ON pat.NHS_NUMBER_DEID = death.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'HES-APC') hes_apc ON pat.NHS_NUMBER_DEID = hes_apc.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'GDPPR') gdppr ON pat.NHS_NUMBER_DEID = gdppr.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.patient_mi_dataset_flags
# MAGIC )
# MAGIC   SELECT "Primary & NOT HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 1 and mi_hes_apc = 0 and mi_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 1 and mi_hes_apc = 1 and mi_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 0 and mi_hes_apc = 1 and mi_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 0 and mi_hes_apc = 1 and mi_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 0 and mi_hes_apc = 0 and mi_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 1 and mi_hes_apc = 0 and mi_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 1 and mi_hes_apc = 1 and mi_death = 1
# MAGIC Union All
# MAGIC   SELECT "Primary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_gdppr = 1
# MAGIC union all
# MAGIC   SELECT "Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_hes_apc = 1
# MAGIC Union All
# MAGIC   SELECT "Death_table" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_CVDstroke FROM CTE WHERE mi_death = 1

# COMMAND ----------

# MAGIC %md ## Fig 2c Reported or suspected Covid test 
# MAGIC 
# MAGIC We need where:
# MAGIC - patient reported to have covid in GDPPR
# MAGIC - patient reported to have covid in HES_APC
# MAGIC - patient reported to have covid in deaths
# MAGIC - where they had something in any of the above
# MAGIC - where they were in SGSS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW patient_covid_19_dataset_flags AS
# MAGIC 
# MAGIC WITH CTE as (SELECT NHS_NUMBER_DEID, Source FROM dars_nic_391419_j3w9t_collab.dp_covid19_test_all3 )
# MAGIC 
# MAGIC SELECT pat.*, 
# MAGIC   CASE WHEN covid_sgss.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as covid_sgss,
# MAGIC   CASE WHEN covid_hes_apc.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as covid_hes_apc,
# MAGIC   CASE WHEN covid_gdppr.NHS_NUMBER_DEID IS NOT NULL THEN 1 ELSE 0 END as covid_gdppr,
# MAGIC   CASE WHEN covid_death.DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL THEN 1 ELSE 0 END as covid_death
# MAGIC FROM global_temp.alive_patients_2020 pat
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'SGSS') covid_sgss ON pat.NHS_NUMBER_DEID = covid_sgss.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'HES APC') covid_hes_apc ON pat.NHS_NUMBER_DEID = covid_hes_apc.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT * FROM CTE WHERE source = 'GDPPR') covid_gdppr ON pat.NHS_NUMBER_DEID = covid_gdppr.NHS_NUMBER_DEID
# MAGIC     LEFT JOIN (SELECT DISTINCT PERSON_ID_DEID as DEC_CONF_NHS_NUMBER_CLEAN_DEID FROM dars_nic_391419_j3w9t_collab.dp_covid_death) covid_death ON pat.NHS_NUMBER_DEID = covid_death.DEC_CONF_NHS_NUMBER_CLEAN_DEID

# COMMAND ----------

# MAGIC %md Then do the counts:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.patient_covid_19_dataset_flags
# MAGIC )
# MAGIC   SELECT "Primary & NOT HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & NOT Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 0
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "NOT Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 0 and covid_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & NOT HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & HES & Deaths" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 1
# MAGIC Union All
# MAGIC   SELECT "Primary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1
# MAGIC union all
# MAGIC   SELECT "Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_hes_apc = 1
# MAGIC Union All
# MAGIC   SELECT "Death_table" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_death = 1
# MAGIC Union All
# MAGIC   SELECT "(Primary OR Secondary OR Deaths_table) AND NOT sgss" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE 
# MAGIC   WHERE (covid_gdppr = 1 OR (covid_death = 1) OR covid_hes_apc = 1) AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE 
# MAGIC   WHERE (covid_gdppr = 0 AND (covid_death = 0 ) AND covid_hes_apc = 0) AND covid_sgss = 1
# MAGIC UNION ALL
# MAGIC   SELECT "(Primary OR Secondary OR Deaths_table) & SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE 
# MAGIC   WHERE covid_sgss = 1 AND (covid_gdppr = 1 OR (covid_death = 1) OR covid_hes_apc = 1)

# COMMAND ----------

# MAGIC %md 4 way Venn Diagram (GDPPR, HES, SGSS, Deaths)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.patient_covid_19_dataset_flags
# MAGIC )
# MAGIC   SELECT "Primary only" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 0 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "HES only" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 0 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "Deaths only" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 0 and covid_death = 1 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "SGSS only" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 0 and covid_death = 0 AND covid_sgss=1
# MAGIC UNION all
# MAGIC   SELECT "Primary & HES ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 0 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & Death ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 1 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary & SGSS ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 0 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "HES & Death ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 1 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "HES & SGSS ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 0 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "Death & SGSS ONLY" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 0 and covid_death = 1 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary, HES & DEATH" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 1 AND covid_sgss=0
# MAGIC UNION ALL
# MAGIC   SELECT "Primary, HES & SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 0 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary, Death & SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 0 and covid_death = 1 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "HES, Death & SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 0 and covid_hes_apc = 1 and covid_death = 1 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary, HES, Death & SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1 and covid_hes_apc = 1 and covid_death = 1 AND covid_sgss=1
# MAGIC UNION ALL
# MAGIC   SELECT "Primary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_gdppr = 1
# MAGIC union all
# MAGIC   SELECT "Secondary" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_hes_apc = 1
# MAGIC Union All
# MAGIC   SELECT "Death_table" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_death = 1
# MAGIC Union All
# MAGIC    SELECT "SGSS" as dataset, COUNT(DISTINCT NHS_NUMBER_DEID) as patients_alive_1stjan2020_with_covid FROM CTE WHERE covid_sgss = 1
