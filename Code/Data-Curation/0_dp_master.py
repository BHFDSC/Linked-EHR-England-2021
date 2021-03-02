# Databricks notebook source
# MAGIC %md # Descriptive Paper Master Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Description** Runs the notebooks which produce the descriptive paper in sequence. These are:
# MAGIC - DP_skinny_patient_01_01_2020
# MAGIC - Stroke_MI_event
# MAGIC - MI_Stroke_previous event
# MAGIC - samantha_covariate_flags
# MAGIC - COVID-19 testing table (1) (1)
# MAGIC - sam_h_fig_2_venn
# MAGIC - rachel_table3
# MAGIC 
# MAGIC Final results are in:
# MAGIC  
# MAGIC **Project(s)** Descriptive Paper
# MAGIC  
# MAGIC **Author(s)** Sam Hollings
# MAGIC  
# MAGIC **Reviewer(s)** Angela Wood
# MAGIC  
# MAGIC **Date last updated** 2021-01-6
# MAGIC  
# MAGIC **Date last reviewed** 2021-01-6
# MAGIC  
# MAGIC **Date last run** 2021-01-6
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC **Software and versions** Replace this with the software running the code in the notebook (and any version requirements), e.g. SQL, Python, R (>= 3.5.0)
# MAGIC  
# MAGIC **Packages and versions** Replace this text with a list of packages used in this notebook (and if required any version requirements for these) or state 'Not applicable'

# COMMAND ----------

# MAGIC %md **Populate the "dp_" tables**
# MAGIC Using batch:
# MAGIC - bcaf6e02-a878-44e3-98d8-cc77f5ab2ef2 (december)

# COMMAND ----------

timeout = 60*60 # 1 hour
# This only needs to be run when the data has been updated, or when you want to change the batch.
#
# dbutils.notebook.run("samh_dp_data_wrangle_master", timeout)

# COMMAND ----------

# MAGIC %md First need to make the DP skinny patient table (`DP_skinny_patient_01_01_2020`)

# COMMAND ----------

# don't need to run this every time as it has already been made for this batch
# dbutils.notebook.run("DP_skinny_patient_01_01_2020", timeout)

# COMMAND ----------

# MAGIC %md Define the data to be used

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_hes_apc_all
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.dp_hes_apc_all_years

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_gdppr
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.dp_gdppr_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_deaths
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.dp_deaths_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_sgss
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.dp_sgss_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_primarycare_meds
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.primary_care_meds_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view dp_skinny
# MAGIC as select *
# MAGIC from dars_nic_391419_j3w9t_collab.dp_skinny_patient_01_01_2020

# COMMAND ----------

# MAGIC %md Make flags corresponding with first stroke or MI events 
# MAGIC - "Stroke_MI_event" notebook -> `6_dp_Stroke_MI_incidence`
# MAGIC - "MI_Stroke_previous event" notebook -> `5_dp_MI_Stroke_previous event`

# COMMAND ----------

dbutils.notebook.run("5_dp_MI_Stroke_previous event", timeout)

# COMMAND ----------

dbutils.notebook.run("6_dp_Stroke_MI_incidence", timeout)

# COMMAND ----------

# MAGIC %md get diabetes and other coveriate flags (`samantha_covariate_flags_diabetes`, `samantha_covariate_flags_obesity`)

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.run("7_dp_covariate_flags", timeout)

# COMMAND ----------

dbutils.notebook.run("8_dp_deaths", timeout)

# COMMAND ----------

# MAGIC %md Make flags corresponding with COVID19 events (`covid19_test_all3`, `covid_cc_death_flags` and `patient_covid_cause_death`:

# COMMAND ----------

dbutils.notebook.run("4_dp_COVID-19", timeout)

# COMMAND ----------

# MAGIC %md make the Venn diagrams:

# COMMAND ----------

dbutils.notebook.run("dp_Fig_2_venn", timeout)

# COMMAND ----------

# MAGIC %md calculate the values for table 2:

# COMMAND ----------

dbutils.notebook.run("dp_table2", timeout)

# COMMAND ----------

# MAGIC %md calculate the values for table 2:

# COMMAND ----------

dbutils.notebook.run("dp_table3", timeout)
