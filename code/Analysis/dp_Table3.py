# Databricks notebook source
# MAGIC %md # dp_Table3: Descriptive paper, Table 3
# MAGIC  
# MAGIC **Description** This notebooks generates the output for table 3 in the descriptive paper. This has been derived from:
# MAGIC - skinny table - which version?
# MAGIC - covid19_test_all3
# MAGIC 
# MAGIC Have updated to include Samatha's **samantha_xmas_check_deaths**, which is where the counts for deaths originate from. Implemented in the cell which makes the "table_3" global temp view.
# MAGIC 
# MAGIC 
# MAGIC **Project(s)** Descriptive Manuscript
# MAGIC  
# MAGIC **Author(s)** Rachel Denholm
# MAGIC  
# MAGIC **Reviewer(s)** Sam Hollings
# MAGIC  
# MAGIC **Date last updated** 2020-12-21
# MAGIC  
# MAGIC **Date last reviewed** 2020-12-21
# MAGIC  
# MAGIC **Date last run** 2020-12-21
# MAGIC 
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC 
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %md
# MAGIC | Characteristic                         | Category | Total | Reported/suspected COVID-19 +ve from primary care records (from GDPPR oly) | PHE SGSS COVID-19 positive antigen test (N, %) | Admitted to hospital with confirmed or suspected COVID-19 (from HES-APC only) | Admitted to ICU with confirmed or suspected COVID-19 (from HES-CC only)  | Death related to COVID-19  (from death records only)  |
# MAGIC |----------------------------------------|----------|-------|-----------------------------------------|-----------------------------------------------------------|------------------------------------------------------|-------------------------------------------------|-------------------------------------------------|
# MAGIC | Total patients*                        |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Sex                                    |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Age group                              |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Ethnicity                              |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Previous MI/Stroke (GDPPR/HES)        |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Previously diagnosed Diabetes (GDPPR/HES) |          |       |                                         |                                                           |                                                      |                                                 |                                                 |
# MAGIC | Obese (GDPPR) |          |       |                                         |                                                           |                                                      |                                                 |                                                 |

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Patients alive 1st Jan 2020, data from skinny table and covid-19 outcomes
# MAGIC --- skinny table needs updating to those alive 1st Jan 2020 when complete*******************
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW table3 AS
# MAGIC 
# MAGIC --- single record per patient per source
# MAGIC with cte_covid as (
# MAGIC select distinct COALESCE(covid_sgss_gdppr.nhs_number_deid, covid_hes_cc_deaths.PERSON_ID_DEID) as id, 
# MAGIC     covid_sgss_gdppr.Source, covid_sgss_gdppr.Covid19_status, covid_sgss_gdppr.pillar, covid_sgss_gdppr.Status,
# MAGIC     covid_hes_cc_deaths.Admitted_hospital_confirm_suspect_covid,
# MAGIC     covid_hes_cc_deaths.Admitted_ICU_confirm_suspect_covid
# MAGIC from dars_nic_391419_j3w9t_collab.dp_covid19_test_all3 covid_sgss_gdppr
# MAGIC FULL OUTER JOIN
# MAGIC   dars_nic_391419_j3w9t_collab.dp_covid_cc_death_flags covid_hes_cc_deaths ON covid_sgss_gdppr.nhs_number_deid = covid_hes_cc_deaths.PERSON_ID_DEID -- this table comes from "COVID-19 testing table_updated" notebook
# MAGIC )
# MAGIC SELECT skinny.*, covid.*, CASE WHEN dp_covid_death.DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL THEN 1 ELSE NULL END AS Death_related_covid19 -- Using same as Venn Diagram
# MAGIC FROM global_temp.dp_skinny skinny
# MAGIC LEFT JOIN cte_covid covid ON skinny.NHS_NUMBER_DEID = covid.id
# MAGIC LEFT JOIN
# MAGIC    (SELECT DISTINCT PERSON_ID_DEID as DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC    FROM 
# MAGIC    dars_nic_391419_j3w9t_collab.dp_covid_death
# MAGIC    ) dp_covid_death ON skinny.NHS_NUMBER_DEID = dp_covid_death.DEC_CONF_NHS_NUMBER_CLEAN_DEID

# COMMAND ----------

# MAGIC %md ### Sex

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW sex_lookup AS
# MAGIC SELECT *, 
# MAGIC       CASE WHEN SEX_CODE = 1 THEN "Male"
# MAGIC            WHEN SEX_CODE = 2 THEN "Female" 
# MAGIC            WHEN SEX_CODE = 9 THEN "Indeterminate"
# MAGIC            ELSE 'Unknown' END as SEX_GROUP  
# MAGIC FROM (
# MAGIC   SELECT Value as SEX_CODE FROM dss_corporate.gdppr_sex  
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(SEX_GROUP, 'Unknown') as SEX_GROUP, 
# MAGIC   COUNT(DISTINCT nhs_number_deid) as patients,
# MAGIC   COUNT(DISTINCT IF(Source = 'GDPPR' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19', NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19' AND pillar = 'pillar_1', NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(Source = 'HES APC' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(Admitted_ICU_confirm_suspect_covid = 1, NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(Death_related_covid19 = 1, NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.table3 
# MAGIC     LEFT JOIN global_temp.sex_lookup ON SEX =  SEX_CODE
# MAGIC GROUP BY COALESCE(SEX_GROUP, 'Unknown')
# MAGIC ORDER BY COALESCE(SEX_GROUP, 'Unknown')

# COMMAND ----------

# MAGIC %md ### Ethnicity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COALESCE(CATEGORISED_ETHNICITY, 'Unknown') as CATEGORISED_ETHNICITY, 
# MAGIC   COUNT(DISTINCT nhs_number_deid) as patients,
# MAGIC   COUNT(DISTINCT IF(Source = 'GDPPR' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19', NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19' AND pillar = 'pillar_1', NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(Source = 'HES APC' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(Admitted_ICU_confirm_suspect_covid = 1, NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(Death_related_covid19 = 1, NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.table3  
# MAGIC GROUP BY COALESCE(CATEGORISED_ETHNICITY, 'Unknown')
# MAGIC ORDER BY COALESCE(CATEGORISED_ETHNICITY, 'Unknown')

# COMMAND ----------

# MAGIC %md ### AGE GROUP

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW agegrp_lookup AS
# MAGIC SELECT *, 
# MAGIC       CASE WHEN AGE_AT_COHORT_START >= 0 and AGE_AT_COHORT_START<18 THEN "0-17"
# MAGIC            WHEN AGE_AT_COHORT_START > 17 and AGE_AT_COHORT_START<30 THEN "18-29" 
# MAGIC            WHEN AGE_AT_COHORT_START > 29 and AGE_AT_COHORT_START<50 THEN "30-49" 
# MAGIC            WHEN AGE_AT_COHORT_START > 49 and AGE_AT_COHORT_START<70 THEN "50-69" 
# MAGIC            WHEN AGE_AT_COHORT_START > 69 and AGE_AT_COHORT_START<120 THEN "70+"
# MAGIC            ELSE 'Unknown' END as AGE_GROUP  
# MAGIC FROM global_temp.table3  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(AGE_GROUP, 'Unknown') as AGE_GROUP, 
# MAGIC   COUNT(DISTINCT nhs_number_deid) as patients,
# MAGIC   COUNT(DISTINCT IF(Source = 'GDPPR' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19', NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(Source = 'SGSS' AND Status = 'Confirmed COVID19' AND pillar = 'pillar_1', NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(Source = 'HES APC' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(Admitted_ICU_confirm_suspect_covid = 1, NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(Death_related_covid19 = 1, NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.agegrp_lookup  
# MAGIC GROUP BY COALESCE(AGE_GROUP, 'Unknown') 
# MAGIC ORDER BY COALESCE(AGE_GROUP, 'Unknown') 

# COMMAND ----------

# MAGIC %md ### PREVIOUS DIAGNOSIS OF MI

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- single record per patient per source
# MAGIC with cte_covid as (
# MAGIC select distinct NHS_NUMBER_DEID as id, Source, Covid19_status, pillar, Status, Admitted_hospital_confirm_suspect_covid,
# MAGIC     Admitted_ICU_confirm_suspect_covid,
# MAGIC     Death_related_covid19
# MAGIC from global_temp.table3
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC SELECT previous.*, covid.*
# MAGIC FROM dars_nic_391419_j3w9t_collab.descriptive_mi_previous_distinct_table previous
# MAGIC LEFT JOIN cte_covid covid ON previous.PERSON_ID_DEID = covid.id
# MAGIC ),
# MAGIC 
# MAGIC query as (
# MAGIC SELECT  
# MAGIC COALESCE(MI_Previous, 0) as MI_Previous,
# MAGIC   COUNT(DISTINCT skinny.NHS_NUMBER_DEID) as patients,
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'GDPPR' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19', skinny.NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19' AND skinny.pillar = 'pillar_1', skinny.NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'HES APC' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Admitted_ICU_confirm_suspect_covid = 1, skinny.NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Death_related_covid19 = 1, skinny.NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.table3 skinny LEFT JOIN cte_join on skinny.NHS_NUMBER_DEID = cte_join.PERSON_ID_DEID 
# MAGIC GROUP BY COALESCE(MI_Previous, 0)
# MAGIC ORDER BY COALESCE(MI_Previous, 0))
# MAGIC 
# MAGIC SELECT * from query
# MAGIC Union all
# MAGIC SELECT "Total" as EVER_DIAB, sum(patients) as patients, sum(GDPPR_covid) as GDPPR_covid, sum(SGSS) as SGSS, sum(SGSS_Pilar1) as SGSS_Pilar1, sum(HESAPC_Covid) as HESAPC_Covid, sum(HESCC_Covid) as HESCC_Covid, sum(Death_Covid) as Death_Covid FROM query

# COMMAND ----------

# MAGIC %md ### PREVIOUS DIAGNOSIS OF Stroke

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- single record per patient per source
# MAGIC with cte_covid as (
# MAGIC select distinct NHS_NUMBER_DEID as id, Source, Covid19_status, pillar, Status, Admitted_hospital_confirm_suspect_covid,
# MAGIC     Admitted_ICU_confirm_suspect_covid,
# MAGIC     Death_related_covid19
# MAGIC from global_temp.table3
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC SELECT previous.*, covid.*
# MAGIC FROM dars_nic_391419_j3w9t_collab.descriptive_stroke_previous_distinct_table previous
# MAGIC LEFT JOIN cte_covid covid ON previous.PERSON_ID_DEID = covid.id
# MAGIC ),
# MAGIC 
# MAGIC query as (
# MAGIC SELECT  
# MAGIC COALESCE(Stroke_Previous, 0) as Stroke_Previous,
# MAGIC   COUNT(DISTINCT skinny.NHS_NUMBER_DEID) as patients,
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'GDPPR' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19', skinny.NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19' AND skinny.pillar = 'pillar_1', skinny.NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'HES APC' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Admitted_ICU_confirm_suspect_covid = 1, skinny.NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Death_related_covid19 = 1, skinny.NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.table3 skinny LEFT JOIN cte_join on skinny.NHS_NUMBER_DEID = cte_join.PERSON_ID_DEID 
# MAGIC GROUP BY COALESCE(Stroke_Previous, 0)
# MAGIC ORDER BY COALESCE(Stroke_Previous, 0))
# MAGIC 
# MAGIC SELECT * from query
# MAGIC Union all
# MAGIC SELECT "Total" as EVER_DIAB, sum(patients) as patients, sum(GDPPR_covid) as GDPPR_covid, sum(SGSS) as SGSS, sum(SGSS_Pilar1) as SGSS_Pilar1, sum(HESAPC_Covid) as HESAPC_Covid, sum(HESCC_Covid) as HESCC_Covid, sum(Death_Covid) as Death_Covid FROM query

# COMMAND ----------

# MAGIC %md ### PREVIOUS DIAGNOSIS OF DIABETES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- single record per patient per source
# MAGIC with cte_covid as (
# MAGIC select distinct NHS_NUMBER_DEID, Source, Covid19_status, pillar, Status, Admitted_hospital_confirm_suspect_covid,
# MAGIC     Admitted_ICU_confirm_suspect_covid,
# MAGIC     Death_related_covid19
# MAGIC from global_temp.table3
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC SELECT diabetes.*, covid.*
# MAGIC FROM dars_nic_391419_j3w9t_collab.covariate_flags_diabetes diabetes
# MAGIC LEFT JOIN cte_covid covid ON diabetes.ID = covid.NHS_NUMBER_DEID
# MAGIC ),
# MAGIC query as (
# MAGIC SELECT   
# MAGIC COALESCE(EVER_DIAB, 0, null) as EVER_DIAB, 
# MAGIC   COUNT(DISTINCT skinny.nhs_number_deid) as patients,
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'GDPPR' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19', skinny.NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19' AND skinny.pillar = 'pillar_1', skinny.NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'HES APC' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Admitted_ICU_confirm_suspect_covid = 1, skinny.NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Death_related_covid19 = 1, skinny.NHS_NUMBER_DEID, NULL)) as Death_Covid
# MAGIC FROM global_temp.table3 skinny LEFT JOIN cte_join on skinny.NHS_NUMBER_DEID = cte_join.NHS_NUMBER_DEID 
# MAGIC --FROM cte_join 
# MAGIC GROUP BY COALESCE(EVER_DIAB, 0, null)
# MAGIC ORDER BY COALESCE(EVER_DIAB, 0, null))
# MAGIC 
# MAGIC SELECT * from query
# MAGIC Union all
# MAGIC SELECT "Total" as EVER_DIAB, sum(patients) as patients, sum(GDPPR_covid) as GDPPR_covid, sum(SGSS) as SGSS, sum(SGSS_Pilar1) as SGSS_Pilar1, sum(HESAPC_Covid) as HESAPC_Covid, sum(HESCC_Covid) as HESCC_Covid, sum(Death_Covid) as Death_Covid FROM query

# COMMAND ----------

# MAGIC %md ### OBESITY 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- single record per patient per source
# MAGIC with cte_covid as (
# MAGIC select distinct NHS_NUMBER_DEID, Source, Covid19_status, pillar, Status	,Admitted_hospital_confirm_suspect_covid,
# MAGIC     Admitted_ICU_confirm_suspect_covid,
# MAGIC     Death_related_covid19
# MAGIC from global_temp.table3
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC SELECT obesity.*, covid.*
# MAGIC FROM dars_nic_391419_j3w9t_collab.covariate_flags_obesity obesity
# MAGIC LEFT JOIN cte_covid covid ON obesity.ID = covid.NHS_NUMBER_DEID
# MAGIC ),
# MAGIC query as (
# MAGIC SELECT 
# MAGIC -- EVER_OBESE, 
# MAGIC --   COUNT(DISTINCT NHS_NUMBER_DEID) as patients,
# MAGIC --   COUNT(IF(Source = 'GDPPR' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), 1, NULL)) as GDPPR_covid, 
# MAGIC --   COUNT(IF(Source = 'SGSS' AND Status = 'Confirmed COVID19' AND pillar = 'pillar_1', 1, NULL)) as SGSS_Pilar1, 
# MAGIC --   COUNT(IF(Source = 'HES APC' AND (Status = 'Confirmed COVID19' OR Status = 'Suspected COVID19'), 1, NULL)) as HESAPC_Covid
# MAGIC   
# MAGIC  COALESCE(EVER_OBESE, 0) as EVER_OBESE, 
# MAGIC   COUNT(DISTINCT skinny.nhs_number_deid) as patients,
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'GDPPR' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as GDPPR_covid, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19', skinny.NHS_NUMBER_DEID, NULL)) as SGSS, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'SGSS' AND skinny.Status = 'Confirmed COVID19' AND skinny.pillar = 'pillar_1', skinny.NHS_NUMBER_DEID, NULL)) as SGSS_Pilar1, 
# MAGIC   COUNT(DISTINCT IF(skinny.Source = 'HES APC' AND (skinny.Status = 'Confirmed COVID19' OR skinny.Status = 'Suspected COVID19'), skinny.NHS_NUMBER_DEID, NULL)) as HESAPC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Admitted_ICU_confirm_suspect_covid = 1, skinny.NHS_NUMBER_DEID, NULL)) as HESCC_Covid,
# MAGIC   COUNT(DISTINCT IF(skinny.Death_related_covid19 = 1, skinny.NHS_NUMBER_DEID, NULL)) as Death_Covid 
# MAGIC --FROM cte_join 
# MAGIC FROM global_temp.table3 skinny LEFT JOIN cte_join on skinny.NHS_NUMBER_DEID = cte_join.NHS_NUMBER_DEID 
# MAGIC GROUP BY  COALESCE(EVER_OBESE, 0)
# MAGIC ORDER BY  COALESCE(EVER_OBESE, 0)
# MAGIC )
# MAGIC SELECT * from query
# MAGIC Union all
# MAGIC SELECT "Total" as EVER_DIAB, sum(patients) as patients, sum(GDPPR_covid) as GDPPR_covid, sum(SGSS) as SGSS, sum(SGSS_Pilar1) as SGSS_Pilar1, sum(HESAPC_Covid) as HESAPC_Covid, sum(HESCC_Covid) as HESCC_Covid, sum(Death_Covid) as Death_Covid FROM query

# COMMAND ----------


