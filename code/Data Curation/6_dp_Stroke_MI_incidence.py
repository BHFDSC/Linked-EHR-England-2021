# Databricks notebook source
# MAGIC %md
# MAGIC  # 6_dp_Stroke_MI_Incidence: First record of stroke and MI event from 1st Jan 2020
# MAGIC  
# MAGIC **Description** This notebooks make a table `dars_nic_391419_j3w9t_collab.descriptive_Stroke_MI_event` containing the patients alive 1st Jan 2020 (identified using `patient_skinny_record_2`) who have had a first report of an MI or stroke **after** 31st Dec 2019 (), in each of the data tables below:
# MAGIC - HES APC;  
# MAGIC - GDPPR;  
# MAGIC - Death.
# MAGIC 
# MAGIC **CALIBER Codes used**
# MAGIC - caliber_icd_myocardial_infarction; 
# MAGIC - caliber_icd_isch_stroke; 
# MAGIC - caliber_icd_stroke_nos; 
# MAGIC - caliber_icd_tia; 
# MAGIC - caliber_icd_intracereb_haem;
# MAGIC - bhfcvdcovid_stroke_is_snomed;
# MAGIC - bhfcvdcovid_stroke_nos_snomed;
# MAGIC - bhfcvdcovid_stroke_tia_snomed;
# MAGIC - bhfcvdcovid_mi_snomed.
# MAGIC 
# MAGIC Subarach haem desired but codes currently unavailable in GDPPR data so removed here
# MAGIC 
# MAGIC **Project(s)** Descriptive Manuscript
# MAGIC  
# MAGIC **Author(s)** Rachel Denholm
# MAGIC  
# MAGIC **Reviewer(s)** Sam Hollings
# MAGIC  
# MAGIC **Date last updated** 2020-12-17 (cmd 17 updated)
# MAGIC  
# MAGIC **Date last reviewed** 2020-12-17
# MAGIC  
# MAGIC **Date last run** 2020-12-17
# MAGIC  
# MAGIC **Data input** GDPPR; HES APC; Deaths
# MAGIC 
# MAGIC **Data output** tables: 
# MAGIC - dp_firststroke_event_table  
# MAGIC - dp_firstmi_event_table
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC  
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will create a single table/view that contains an observation for each first MI/Stroke clinical code record (multiple patient records) - see table below 'dp_firstmi_event_table/dp_firststroke_event_table' for all patients that had an event after 31st Dec 2019
# MAGIC 
# MAGIC |Column | Content|
# MAGIC |----------------|--------------------|
# MAGIC |PERSON_ID_DEID | Patient NHS Number |
# MAGIC |DATE | Date of event (DATE in GDPPR, EPISTART in HES) |
# MAGIC |Source | GDPPR, HES-APC, Death |

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.dp_firstmi_event_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.dp_firststroke_event_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Temporary View with the BHF CALIBER validated codes - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW dars_nic_391419_j3w9t_collab.validated_MI_Stroke_SNOMED AS
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("88032003","Amaurosis fugax","Incidence","TIA"),
# MAGIC ("195206000","Intermittent cerebral ischaemia","Incidence","TIA"),
# MAGIC ("230716006","Anterior circulation transient ischaemic attack","Incidence","TIA"),
# MAGIC ("266257000","TIA","Incidence","TIA"),
# MAGIC ("710575003","Transient ischemic attack due to embolism","Incidence","TIA"),
# MAGIC ("140221000119109","History of transient ischemic attack due to embolism (situation)","History","TIA"),
# MAGIC ("13016361000119101","History of amaurosis fugax (situation)","History","TIA"),
# MAGIC ("6594005","Cerebrovascular disorder in the puerperium","Incidence","Stroke NOS"),
# MAGIC ("170600009","Stroke monitoring","Management","Stroke NOS"),
# MAGIC ("195212005","Brainstem stroke syndrome","Incidence","Stroke NOS"),
# MAGIC ("195213000","Cerebellar stroke syndrome","Incidence","Stroke NOS"),
# MAGIC ("195216008","Left sided cerebral hemisphere cerebrovascular accident","Incidence","Stroke NOS"),
# MAGIC ("195217004","Right sided CVA","Incidence","Stroke NOS"),
# MAGIC ("195239002","Late effects of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("195240000","Sequelae of subarachnoid hemorrhage","History","Stroke NOS"),
# MAGIC ("195608005","[X]Sequelae of stroke, not specified as haemorrhage or infarction","History","Stroke NOS"),
# MAGIC ("230690007","CVA - Cerebrovascular accident","Incidence","Stroke NOS"),
# MAGIC ("275434003","Stroke in the puerperium","Incidence","Stroke NOS"),
# MAGIC ("275526006","H/O: CVA","History","Stroke NOS"),
# MAGIC ("275527002","H/O: stroke","History","Stroke NOS"),
# MAGIC ("277286006","CPSP - Central post-stroke pain","History","Stroke NOS"),
# MAGIC ("308067002","H/O: Stroke in last year","History","Stroke NOS"),
# MAGIC ("425642008","Monoplegia of dominant lower limb as a late effect of cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("425882004","Paralytic syndrome as late effect of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("426033005","Dysphagia as a late effect of cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("426788002","Vertigo as late effect of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("427065003","Monoplegia of dominant upper limb as a late effect of cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("427432001","Paralytic syndrome as late effect of thalamic stroke (disorder)","History","Stroke NOS"),
# MAGIC ("428668000","Apraxia due to cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("429993008","History of cerebrovascular accident without residual deficits (situation)","History","Stroke NOS"),
# MAGIC ("430947007","Paralytic syndrome of nondominant side as late effect of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("430959006","Paralytic syndrome of dominant side as late effect of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("431310008","History of occlusion of cerebral artery (situation)","History","Stroke NOS"),
# MAGIC ("433183000","Neurogenic bladder as late effect of cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("440140008","History of cerebrovascular accident with residual deficit (situation)","History","Stroke NOS"),
# MAGIC ("441529001","Dysphasia as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("441630004","Aphasia as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("441735003","Sensory disorder as a late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("441759008","Abnormal vision as a late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("441887006","Monoplegia of lower limb as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("441894009","Monoplegia of nondominant lower limb as a late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("441960006","Speech and language deficit as late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("441991000","Hemiparesis as late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("442024001","Hemiplegia as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("442097001","Monoplegia of upper limb as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("442181008","Monoplegia of nondominant upper limb as a late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("442212003","Residual cognitive deficit as late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("442617003","Aphasia as late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("442668000","Hemiplegia of nondominant side as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("442676003","Hemiplegia of dominant side as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("442733008","Hemiplegia as late effect of cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("698767004","Post-cerebrovascular accident epilepsy","History","Stroke NOS"),
# MAGIC ("699270006","Stroke annual review","Management","Stroke NOS"),
# MAGIC ("699429007","History of cerebrovascular accident in last eight weeks (situation)","History","Stroke NOS"),
# MAGIC ("713410003","Pain following cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("720849008","Education about stroke","Management","Stroke NOS"),
# MAGIC ("722929005","Perinatal arterial ischemic stroke (disorder)","Incidence","Stroke NOS"),
# MAGIC ("40161000119102","Weakness of face muscles as sequela of stroke","History","Stroke NOS"),
# MAGIC ("48601000119107","Hemiplegia and/or hemiparesis following stroke","History","Stroke NOS"),
# MAGIC ("87551000119101","Visual disturbance as sequela of cerebrovascular disease (disorder)","History","Stroke NOS"),
# MAGIC ("92341000119107","Weakness of extremities as sequela of stroke","History","Stroke NOS"),
# MAGIC ("97531000119106","History of parietal cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("99051000119101","History of lacunar cerebrovascular accident","History","Stroke NOS"),
# MAGIC ("102831000119104","Paraplegia or paraparesis as sequela of stroke","History","Stroke NOS"),
# MAGIC ("103761000119107","Paralytic syndrome of all four limbs as sequela of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("133981000119106","Dysarthria as late effects of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("133991000119109","Fluency disorder as sequela of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("134771000119108","Alteration of sensation as late effect of stroke (disorder)","History","Stroke NOS"),
# MAGIC ("137991000119103","Seizure disorder as sequela of stroke","History","Stroke NOS"),
# MAGIC ("140281000119108","Hemiparesis as late effect of cerebrovascular disease","History","Stroke NOS"),
# MAGIC ("148871000119109","Weakness as a late effect of stroke","History","Stroke NOS"),
# MAGIC ("186831000119104","Apraxia due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("290581000119101","Ataxia due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("290621000119101","Cognitive deficit due to and following cerebrovascular disease (disorder)","History","Stroke NOS"),
# MAGIC ("290631000119103","Dysarthria due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("290791000119105","Fluency disorder due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("290931000119108","Monoplegia of lower limb due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("291091000119102","Monoplegia of left nondominant upper limb due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("291111000119105","Monoplegia of right nondominant upper limb due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("291121000119103","Monoplegia of upper limb due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("292851000119109","Lacunar ataxic hemiparesis of right dominant side (disorder)","Incidence","Stroke NOS"),
# MAGIC ("292861000119106","Lacunar ataxic hemiparesis of left dominant side (disorder)","Incidence","Stroke NOS"),
# MAGIC ("330411000119109","Lacunar ataxic hemiparesis of left nondominant side (disorder)","Incidence","Stroke NOS"),
# MAGIC ("330421000119102","Lacunar ataxic hemiparesis of right nondominant side (disorder)","Incidence","Stroke NOS"),
# MAGIC ("690051000119100","History of stroke of cerebellum","History","Stroke NOS"),
# MAGIC ("12242711000119109","Weakness of left facial muscle due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("12242751000119105","Weakness of right facial muscle due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("12367511000119101","Paraplegia due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("15982271000119104","Weakness of right facial muscle due to and following cerebrovascular disease (disorder)","History","Stroke NOS"),
# MAGIC ("15982311000119104","Weakness of left facial muscle due to and following cerebrovascular disease (disorder)","History","Stroke NOS"),
# MAGIC ("16260551000119106","Dysphasia due to and following cerebrovascular accident (disorder)","History","Stroke NOS"),
# MAGIC ("6594005","Cerebrovascular disorder in the puerperium","Incidence","Stroke NOS"),
# MAGIC ("71444005","Cerebral thrombosis","Incidence","Ischaemic stroke"),
# MAGIC ("75543006","Cerebral embolism","Incidence","Ischaemic stroke"),
# MAGIC ("78569004","Posterior inferior cerebellar artery syndrome","Incidence","Ischaemic stroke"),
# MAGIC ("95457000","Brain stem infarction","Incidence","Ischaemic stroke"),
# MAGIC ("95460007","Cerebellar infarction","Incidence","Ischaemic stroke"),
# MAGIC ("195185009","Cerebral infarct due to thrombosis of precerebral arteries","Incidence","Ischaemic stroke"),
# MAGIC ("195186005","Cerebral infarction due to embolism of precerebral arteries","Incidence","Ischaemic stroke"),
# MAGIC ("195189003","Cerebral infarction due to thrombosis of cerebral arteries","Incidence","Ischaemic stroke"),
# MAGIC ("195190007","Cerebral infarction due to embolism of cerebral arteries","Incidence","Ischaemic stroke"),
# MAGIC ("195200006","Carotid artery syndrome hemispheric","Incidence","Ischaemic stroke"),
# MAGIC ("195201005","Multiple and bilateral precerebral artery syndromes","Incidence","Ischaemic stroke"),
# MAGIC ("195209007","Middle cerebral artery syndrome","Incidence","Ischaemic stroke"),
# MAGIC ("195210002","Anterior cerebral artery syndrome","Incidence","Ischaemic stroke"),
# MAGIC ("195211003","Posterior cerebral artery syndrome","Incidence","Ischaemic stroke"),
# MAGIC ("195243003","Sequelae of cerebral infarction","History","Ischaemic stroke"),
# MAGIC ("230691006","CVA - cerebrovascular accident due to cerebral artery occlusion","Incidence","Ischaemic stroke"),
# MAGIC ("230692004","Infarction - precerebral","Incidence","Ischaemic stroke"),
# MAGIC ("230698000","Lacunar infarction","Incidence","Ischaemic stroke"),
# MAGIC ("230699008","Pure motor lacunar syndrome","Incidence","Ischaemic stroke"),
# MAGIC ("230700009","Pure sensory lacunar infarction","Incidence","Ischaemic stroke"),
# MAGIC ("266253001","Precerebral arterial occlusion","Incidence","Ischaemic stroke"),
# MAGIC ("307766002","Left sided cerebral infarction","Incidence","Ischaemic stroke"),
# MAGIC ("307767006","Right sided cerebral infarction","Incidence","Ischaemic stroke"),
# MAGIC ("373606000","Occlusive stroke (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("413102000","Infarction of basal ganglia (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("422504002","Ischemic stroke (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("432504007","Cerebral infarction (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("441526008","Infarct of cerebrum due to iatrogenic cerebrovascular accident","Incidence","Ischaemic stroke"),
# MAGIC ("723083001","Late effects of cerebral ischemic stroke (disorder)","History","Ischaemic stroke"),
# MAGIC ("724424009","Cerebral ischemic stroke due to small artery occlusion (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("724425005","Cerebral ischemic stroke due to intracranial large artery atherosclerosis (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("724426006","Cerebral ischemic stroke due to extracranial large artery atherosclerosis (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("724787004","Epilepsy due to cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("724993002","Cerebral ischemic stroke due to occlusion of extracranial large artery (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("724994008","Cerebral ischemic stroke due to stenosis of extracranial large artery (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("725132001","Ischemic stroke without residual deficits (disorder)","Incidence","Ischaemic stroke"),
# MAGIC ("23671000119107","Sequela of ischemic cerebral infarction (disorder)","History","Ischaemic stroke"),
# MAGIC ("33301000119105","Sequela of cardioembolic stroke","History","Ischaemic stroke"),
# MAGIC ("33331000119103","Sequela of lacunar stroke","History","Ischaemic stroke"),
# MAGIC ("33331000119103","Sequela of lacunar stroke","History","Ischaemic stroke"),
# MAGIC ("46421000119102","Behaviour disorder as sequela of cerebral infarction","History","Ischaemic stroke"),
# MAGIC ("91601000119109","Sequela of thrombotic stroke (disorder)","History","Ischaemic stroke"),
# MAGIC ("118951000119103","History of thrombotic stroke without residual deficits","History","Ischaemic stroke"),
# MAGIC ("118961000119101","History of hemorrhagic cerebrovascular accident without residual deficits (situation)","History","Ischaemic stroke"),
# MAGIC ("125081000119106","Cerebral infarction due to occlusion of precerebral artery","Incidence","Ischaemic stroke"),
# MAGIC ("140701000119108","History of hemorrhagic stroke with hemiparesis (situation)","History","Ischaemic stroke"),
# MAGIC ("140711000119106","History of hemorrhagic stroke with hemiplegia","History","Ischaemic stroke"),
# MAGIC ("140911000119109","Ischaemic stroke with coma","Incidence","Ischaemic stroke"),
# MAGIC ("140921000119102","Ischaemic stroke without coma","Incidence","Ischaemic stroke"),
# MAGIC ("141281000119101","History of ischemic stroke without residual deficits (situation)","History","Ischaemic stroke"),
# MAGIC ("141811000119106","History of hemorrhagic cerebrovascular accident with residual deficit (situation)","History","Ischaemic stroke"),
# MAGIC ("141821000119104","History of ischemic cerebrovascular accident with residual deficit (situation)","History","Ischaemic stroke"),
# MAGIC ("141831000119101","History of embolic stroke with deficits","History","Ischaemic stroke"),
# MAGIC ("145741000119101","Apraxia as late effect of cerebrovascular disease","History","Ischaemic stroke"),
# MAGIC ("293831000119105","Cerebral infarction due to stenosis of precerebral artery","Incidence","Ischaemic stroke"),
# MAGIC ("672441000119103","Hemiplegia of nondominant side due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("672461000119104","Hemiplegia of dominant side due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("672501000119104","Dysarthria due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("672521000119108","Dysphasia due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("672541000119102","Aphasia due to and following ischaemic cerebrovascular accident","History","Ischaemic stroke"),
# MAGIC ("672561000119103","Cognitive deficit due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674091000119108","Vertigo due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674111000119100","Ataxia due to and following ischemic cerebrovascular accident","History","Ischaemic stroke"),
# MAGIC ("674121000119107","Ataxia due to and following hemorrhagic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674161000119102","Monoplegia of upper limb due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674361000119104","Apraxia due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674381000119108","Weakness of facial muscle due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674391000119106","Speech and language deficit due to and following hemorrhagic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("674401000119108","Speech and language deficit due to and following ischemic cerebrovascular accident (disorder)","History","Ischaemic stroke"),
# MAGIC ("16896891000119106","History of cerebrovascular accident due to ischemia (situation)","History","Ischaemic stroke"),
# MAGIC ("1508000","Intracerebral hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("7713009","Intrapontine hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("10458001","Evacuation of intracerebral hematoma","Incidence","Hemorrhagic stroke"),
# MAGIC ("28837001","Bulbar hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("49422009","Cortical hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("52201006","Internal capsule hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("75038005","Cerebellar hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("195165005","Basal ganglia hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("195167002","External capsule hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("195168007","Intracerebral haemorrhage, intraventricular","Incidence","Hemorrhagic stroke"),
# MAGIC ("195169004","Intracerebral hemorrhage, multiple localized","Incidence","Hemorrhagic stroke"),
# MAGIC ("195241001","Sequelae of intracerebral haemorrhage","History","Hemorrhagic stroke"),
# MAGIC ("195242008","Sequelae of other non-traumatic intracranial hemorrhage","History","Hemorrhagic stroke"),
# MAGIC ("230710000","Lobar cerebral hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("274100004","Cerebral hemorrhage","Incidence","Hemorrhagic stroke"),
# MAGIC ("308128006","Right sided intracerebral hemorrhage, unspecified","Incidence","Hemorrhagic stroke"),
# MAGIC ("417506008","Hemorrhagic stroke monitoring (regime/therapy)","Incidence","Hemorrhagic stroke"),
# MAGIC ("428267002","History of cerebral hemorrhage (situation)","History","Hemorrhagic stroke"),
# MAGIC ("732923001","Hemorrhage of medulla oblongata (disorder)","Incidence","Hemorrhagic stroke"),
# MAGIC ("118961000119101","History of hemorrhagic cerebrovascular accident without residual deficits (situation)","History","Hemorrhagic stroke"),
# MAGIC ("140701000119108","History of hemorrhagic stroke with hemiparesis (situation)","History","Hemorrhagic stroke"),
# MAGIC ("140711000119106","History of hemorrhagic stroke with hemiplegia","History","Hemorrhagic stroke"),
# MAGIC ("141811000119106","History of hemorrhagic cerebrovascular accident with residual deficit (situation)","History","Hemorrhagic stroke"),
# MAGIC ("145741000119101","Apraxia as late effect of cerebrovascular disease","History","Hemorrhagic stroke"),
# MAGIC ("674121000119107","Ataxia due to and following hemorrhagic cerebrovascular accident (disorder)","History","Hemorrhagic stroke"),
# MAGIC ("674391000119106","Speech and language deficit due to and following hemorrhagic cerebrovascular accident (disorder)","History","Hemorrhagic stroke"),
# MAGIC ("732923001","Hemorrhage of medulla oblongata (disorder)","Incidence","Hemorrhagic stroke"),
# MAGIC ("1755008","Old myocardial infarction","History","Myocardial infarction"),
# MAGIC ("10273003","Acute infarction of papillary muscle","Incidence","Myocardial infarction"),
# MAGIC ("15990001","Acute myocardial infarction of posterolateral wall","Incidence","Myocardial infarction"),
# MAGIC ("22298006","Myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("42531007","Microinfarct of heart","Incidence","Myocardial infarction"),
# MAGIC ("52035003","Acute anteroapical myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("54329005","Acute myocardial infarction of anterior wall","Incidence","Myocardial infarction"),
# MAGIC ("57054005","Acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("58612006","Acute myocardial infarction of lateral wall","Incidence","Myocardial infarction"),
# MAGIC ("62695002","Acute anteroseptal myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("65547006","Acute myocardial infarction of inferolateral wall","Incidence","Myocardial infarction"),
# MAGIC ("66189004","Postmyocardial infarction syndrome","History","Myocardial infarction"),
# MAGIC ("70211005","Acute myocardial infarction of anterolateral wall","Incidence","Myocardial infarction"),
# MAGIC ("70422006","Acute subendocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("71023004","Pericarditis secondary to acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("73795002","Acute myocardial infarction of inferior wall","Incidence","Myocardial infarction"),
# MAGIC ("76593002","Acute myocardial infarction of inferoposterior wall","Incidence","Myocardial infarction"),
# MAGIC ("79009004","Acute myocardial infarction of septum","Incidence","Myocardial infarction"),
# MAGIC ("91335003","Mural thrombus of heart","Incidence","Myocardial infarction"),
# MAGIC ("129574000","Postoperative myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("161502000","H/O: myocardial infarct at less than 60","History","Myocardial infarction"),
# MAGIC ("161503005","H/O: myocardial infarct at greater than 60","History","Myocardial infarction"),
# MAGIC ("164865005","ECG: myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194798004","Acute anteroapical infarction","Incidence","Myocardial infarction"),
# MAGIC ("194802003","True posterior myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194809007","Acute atrial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194856005","Subsequent myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194857001","Subsequent myocardial infarction of anterior wall","Incidence","Myocardial infarction"),
# MAGIC ("194858006","Subsequent myocardial infarction of inferior wall","Incidence","Myocardial infarction"),
# MAGIC ("194861007","Certain current complications following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194862000","Haemopericardium as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194863005","Atrial septal defect as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194865003","Rupture of cardiac wall without hemopericardium as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194866002","Rupture of chordae tendinae as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194867006","Rupture of papillary muscle as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("194868001","Thrombosis of atrium, auricular appendage, and ventricle as current complications following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("233838001","Acute posterior myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("233843008","Silent myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("233846000","Ventricular septal defect as current complication following acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("233847009","Cardiac rupture after acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("233860003","Post-infarction mitral papillary muscle rupture","Incidence","Myocardial infarction"),
# MAGIC ("233885007","Post-infarction pericarditis","Incidence","Myocardial infarction"),
# MAGIC ("233889001","Post-infarction hemopericardium","Incidence","Myocardial infarction"),
# MAGIC ("233929004","Post-infarction mural thrombus","Incidence","Myocardial infarction"),
# MAGIC ("266897007","Family History of myocardial infarction","Family History","Myocardial infarction"),
# MAGIC ("267390005","Post-infarction hypopituitarism","Incidence","Myocardial infarction"),
# MAGIC ("304914007","Acute Q wave myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("307140009","Acute non-Q wave infarction","Incidence","Myocardial infarction"),
# MAGIC ("308065005","H/O: Myocardial infarction in last year","History","Myocardial infarction"),
# MAGIC ("311792005","Postoperative transmural myocardial infarction of anterior wall","Incidence","Myocardial infarction"),
# MAGIC ("311793000","Postoperative transmural myocardial infarction of inferior wall","Incidence","Myocardial infarction"),
# MAGIC ("311796008","Postoperative subendocardial myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("314116003","Post infarct angina","Incidence","Myocardial infarction"),
# MAGIC ("315287002","Diabetes mellitus insulin-glucose infusion in acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("371068009","Myocardial infarction with complication (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("398274000","Coronary artery thrombosis (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("399211009","History of - myocardial infarction (context-dependent category)","History","Myocardial infarction"),
# MAGIC ("401303003","Acute ST segment elevation myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("401314000","Acute non-ST segment elevation myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("428752002","Recent myocardial infarction (situation)","Incidence","Myocardial infarction"),
# MAGIC ("461000119108","History of MI less than 8 weeks","History","Myocardial infarction"),
# MAGIC ("698593009","History of non-ST segment elevation myocardial infarction (situation)","History","Myocardial infarction"),
# MAGIC ("703330009","Mitral valve regurgitation due to acute myocardial infarction with papillary muscle and chordal rupture","Incidence","Myocardial infarction"),
# MAGIC ("703328007","Mitral valve regurgitation due to acute myocardial infarction without papillary muscle and chordal rupture","Incidence","Myocardial infarction"),
# MAGIC ("703326006","Mitral regurgitation due to acute myocardial infarction","Incidence","Myocardial infarction"),
# MAGIC ("103011000119106","Coronary arteriosclerosis in patient with History of previous myocardial infarction (situation)","Incidence","Myocardial infarction"),
# MAGIC ("285721000119104","History of acute ST segment elevation myocardial infarction (situation)","History","Myocardial infarction"),
# MAGIC ("15960981000119105","Mural thrombus of left ventricle following acute myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("723858002","Ventricular aneurysm as current complication following acute myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("723859005","Pulmonary embolism as current complication following acute myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("723860000","Arrhythmia as current complication following acute myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("723861001","Cardiogenic shock unrelated to mechanical complications as current complication following acute myocardial infarction (disorder)","Incidence","Myocardial infarction"),
# MAGIC ("736978009","Mural thrombus of right ventricle following acute myocardial infarction (disorder)","Incidence","Myocardial infarction")
# MAGIC 
# MAGIC AS tab(conceptID, term, Category, Disease);
# MAGIC 
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.validated_MI_Stroke_SNOMED

# COMMAND ----------

# MAGIC  %sql
# MAGIC --- Primary care data
# MAGIC --- Patients with first relevant stroke/MI event codes, based on validated codes 
# MAGIC CREATE OR REPLACE VIEW dars_nic_391419_j3w9t_collab.descriptive_primarycare_StrokeMIevent_firstall AS
# MAGIC 
# MAGIC with cte_gdppr as (
# MAGIC SELECT NHS_NUMBER_DEID as PERSON_ID_DEID, DATE, CODE, "GDPPR" AS SOURCE, "SNOMED" AS MAIN_CODE
# MAGIC FROM global_temp.dp_gdppr
# MAGIC ),
# MAGIC 
# MAGIC cte_join as ( 
# MAGIC select *
# MAGIC from cte_gdppr t1
# MAGIC inner join dars_nic_391419_j3w9t_collab.validated_MI_Stroke_SNOMED t2 on (t1.CODE = t2.conceptId)
# MAGIC where category = "Incidence"
# MAGIC )
# MAGIC 
# MAGIC --- Earlist record of any stroke/MI code
# MAGIC SELECT cj.PERSON_ID_DEID, cj.CODE, cj.term, cj.SOURCE, cj.MAIN_CODE, cj.Disease, cj.Category, cj.DATE
# MAGIC from cte_join cj
# MAGIC INNER JOIN
# MAGIC (
# MAGIC   select MIN(DATE) AS DATE, PERSON_ID_DEID
# MAGIC   FROM cte_join
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC )x ON x.DATE = cj.DATE AND x.PERSON_ID_DEID = cj.PERSON_ID_DEID

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(PERSON_ID_DEID), Disease
# MAGIC From dars_nic_391419_j3w9t_collab.descriptive_primarycare_StrokeMIevent_firstall 
# MAGIC group by Disease
# MAGIC order by Disease

# COMMAND ----------

# MAGIC  %sql
# MAGIC --- HES-APC data
# MAGIC --- Patients with first relevant stroke/MI event codes 
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW dars_nic_391419_j3w9t_collab.descriptive_HESAPC_StrokeMIevent_firstall AS
# MAGIC 
# MAGIC with cte_hes as (
# MAGIC SELECT PERSON_ID_DEID, EPISTART AS DATE, "HES-APC" AS SOURCE, "ICD10" AS MAIN_CODE,
# MAGIC DIAG_3_CONCAT, DIAG_3_01, DIAG_3_02, 
# MAGIC DIAG_3_03, DIAG_3_04, DIAG_3_05, DIAG_3_06, DIAG_3_07, DIAG_3_08, DIAG_3_09, DIAG_3_10, 
# MAGIC DIAG_3_11, DIAG_3_12, DIAG_3_13, DIAG_3_14, DIAG_3_15, DIAG_3_16, DIAG_3_17, DIAG_3_18, 
# MAGIC DIAG_3_19, DIAG_3_20, 
# MAGIC DIAG_4_CONCAT, DIAG_4_01, DIAG_4_02, 
# MAGIC DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
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
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc
# MAGIC FROM global_temp.dp_hes_apc_all
# MAGIC ),
# MAGIC 
# MAGIC cte_icd as (
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_myocardial_infarction
# MAGIC WHERE Category != "History of MI (1)"
# MAGIC UNION
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_isch_stroke
# MAGIC WHERE Category != "History of cerebral infarction"
# MAGIC UNION
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_stroke_nos
# MAGIC WHERE Category != "History of stroke, not specified as haemorrhage or infarction"
# MAGIC UNION
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_tia
# MAGIC UNION
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_intracereb_haem
# MAGIC WHERE Category != "History of intracerebral haemorrhage"
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC select *
# MAGIC from cte_hes t1
# MAGIC inner join cte_icd t2 on (t1.DIAG_3_01 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_02 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_03 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_04 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_05 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_06 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_07 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_08 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_09 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_10 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_11 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_12 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_13 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_14 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_15 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_16 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_17 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_18 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_19 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_20 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_01_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_02_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_03_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_04_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_05_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_06_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_07_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_08_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_09_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_11_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_12_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_13_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_14_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_15_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_16_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_17_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_18_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_19_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_20_trunc = t2.ICD10code_trunc)
# MAGIC )
# MAGIC 
# MAGIC --- Earlist record of any stroke/MI code
# MAGIC SELECT cj.PERSON_ID_DEID, cj.CODE, cj.term, cj.SOURCE, cj.MAIN_CODE, cj.Disease, cj.Category, cj.DATE
# MAGIC from cte_join cj
# MAGIC INNER JOIN
# MAGIC (
# MAGIC   select MIN(DATE) AS DATE, PERSON_ID_DEID
# MAGIC   FROM cte_join
# MAGIC   GROUP BY PERSON_ID_DEID
# MAGIC )x ON x.DATE = cj.DATE AND x.PERSON_ID_DEID = cj.PERSON_ID_DEID

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(PERSON_ID_DEID), Disease
# MAGIC From dars_nic_391419_j3w9t_collab.descriptive_HESAPC_StrokeMIevent_firstall 
# MAGIC group by Disease
# MAGIC order by Disease

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Death data
# MAGIC ---Patients died with Stroke/MI event codes in any position on death certificate from 1st Jan 2020
# MAGIC CREATE OR REPLACE VIEW dars_nic_391419_j3w9t_collab.descriptive_death_StrokeMIevent_all AS
# MAGIC 
# MAGIC with cte_death as (
# MAGIC SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID as PERSON_ID_DEID, REG_DATE_OF_DEATH as DATE, "Death" AS SOURCE, "ICD10" AS MAIN_CODE,
# MAGIC S_UNDERLYING_COD_ICD10, S_COD_CODE_1,
# MAGIC S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8,
# MAGIC S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15,
# MAGIC  LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 3 ) AS S_UNDERLYING_COD_ICD10_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 3 ) AS S_COD_CODE_1_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 3 ) AS S_COD_CODE_2_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 3 ) AS S_COD_CODE_3_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 3 ) AS S_COD_CODE_4_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 3 ) AS S_COD_CODE_5_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 3 ) AS S_COD_CODE_6_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 3 ) AS S_COD_CODE_7_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 3 ) AS S_COD_CODE_8_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 3 ) AS S_COD_CODE_9_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 3 ) AS S_COD_CODE_10_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 3 ) AS S_COD_CODE_11_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 3 ) AS S_COD_CODE_12_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 3 ) AS S_COD_CODE_13_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 3 ) AS S_COD_CODE_14_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 3 ) AS S_COD_CODE_15_trunc
# MAGIC FROM global_temp.dp_deaths
# MAGIC WHERE REG_DATE_OF_DEATH > '2019-12-31'
# MAGIC ),
# MAGIC 
# MAGIC cte_icd as (
# MAGIC SELECT LEFT (REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_myocardial_infarction
# MAGIC WHERE Category != "History of MI (1)"
# MAGIC UNION
# MAGIC SELECT LEFT (REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_isch_stroke
# MAGIC WHERE Category != "History of cerebral infarction"
# MAGIC UNION
# MAGIC SELECT LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_stroke_nos
# MAGIC WHERE Category != "History of stroke, not specified as haemorrhage or infarction"
# MAGIC UNION
# MAGIC SELECT LEFT (REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_tia
# MAGIC UNION
# MAGIC SELECT LEFT (REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_vte_ex_pe
# MAGIC UNION
# MAGIC SELECT LEFT (REGEXP_REPLACE(ICD10code,'[.,-,' ']',''), 4) AS ICD10code_trunc,
# MAGIC Disease, Category, ICD10code AS Code, ICD10codeDescr AS term
# MAGIC FROM bhf_cvd_covid_uk_byod.caliber_icd_intracereb_haem
# MAGIC WHERE Category != "History of intracerebral haemorrhage"
# MAGIC ),
# MAGIC 
# MAGIC cte_join as (
# MAGIC select * 
# MAGIC from cte_death t1
# MAGIC inner join cte_icd t2 on (t1.S_UNDERLYING_COD_ICD10 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_1 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15 = t2.ICD10code_trunc
# MAGIC OR t1.S_UNDERLYING_COD_ICD10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_1_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15_trunc = t2.ICD10code_trunc)
# MAGIC )
# MAGIC 
# MAGIC select PERSON_ID_DEID, to_date(DATE, "yyyyMMdd") as DATE, SOURCE, MAIN_CODE, CODE, term, Disease, Category
# MAGIC from cte_join

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(PERSON_ID_DEID), Disease
# MAGIC From dars_nic_391419_j3w9t_collab.descriptive_death_StrokeMIevent_all
# MAGIC group by Disease
# MAGIC order by Disease

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- ELIGABLE PATIENTS FROM Sam H SKINNY TABLE (only including all those alive 1st Jan 2020), from all three data sources
# MAGIC ---for primary care and HES APC includes first records of each disease
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW dars_nic_391419_j3w9t_collab.descriptive_StrokeMIevent_firstall AS
# MAGIC 
# MAGIC with cte_gdppr AS (
# MAGIC SELECT tab2.PERSON_ID_DEID, tab2.DATE, tab2.SOURCE, tab2.MAIN_CODE, tab2.Disease, tab2.Category, tab2.Code, tab2.term, tab1.NHS_NUMBER_DEID 
# MAGIC from global_temp.dp_skinny tab1
# MAGIC left join dars_nic_391419_j3w9t_collab.descriptive_primarycare_StrokeMIevent_firstall tab2 on tab1.NHS_NUMBER_DEID = tab2.PERSON_ID_DEID
# MAGIC ),
# MAGIC 
# MAGIC cte_hes AS (
# MAGIC SELECT tab2.PERSON_ID_DEID, tab2.DATE, tab2.SOURCE, tab2.MAIN_CODE, tab2.Disease, tab2.Category, tab2.Code, tab2.term, tab1.NHS_NUMBER_DEID 
# MAGIC from global_temp.dp_skinny tab1
# MAGIC left join dars_nic_391419_j3w9t_collab.descriptive_HESAPC_StrokeMIevent_firstall tab2 on tab1.NHS_NUMBER_DEID = tab2.PERSON_ID_DEID
# MAGIC ),
# MAGIC 
# MAGIC cte_death AS (
# MAGIC SELECT tab2.PERSON_ID_DEID, tab2.DATE, tab2.SOURCE, tab2.MAIN_CODE, tab2.Disease, tab2.Category, tab2.Code, tab2.term, tab1.NHS_NUMBER_DEID 
# MAGIC from global_temp.dp_skinny tab1
# MAGIC left join dars_nic_391419_j3w9t_collab.descriptive_death_StrokeMIevent_all tab2 on tab1.NHS_NUMBER_DEID = tab2.PERSON_ID_DEID
# MAGIC )
# MAGIC 
# MAGIC -----create two seperate columns here, one for MI and one for stroke
# MAGIC select PERSON_ID_DEID, DATE, SOURCE, MAIN_CODE, Disease, Category, Code, term,
# MAGIC (case when Disease = "Hemorrhagic stroke" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "TIA" OR Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI_Stroke,
# MAGIC (case when Disease = "Hemorrhagic stroke" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "TIA" THEN 1 Else 0 End) as Stroke,
# MAGIC (case when Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI
# MAGIC from cte_gdppr
# MAGIC UNION
# MAGIC select PERSON_ID_DEID, DATE, SOURCE, MAIN_CODE, Disease, Category, Code, term,
# MAGIC (case when Disease = "Intracerebral haemorrhage" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "Transient ischaemic attack" OR Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI_Stroke,
# MAGIC (case when Disease = "Intracerebral haemorrhage" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "Transient ischaemic attack" THEN 1 Else 0 End) as Stroke,
# MAGIC (case when Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI
# MAGIC from cte_hes
# MAGIC UNION
# MAGIC select PERSON_ID_DEID, DATE, SOURCE, MAIN_CODE, Disease, Category, Code, term,
# MAGIC (case when Disease = "Intracerebral haemorrhage" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "Transient ischaemic attack" OR Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI_Stroke,
# MAGIC (case when Disease = "Intracerebral haemorrhage" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "Transient ischaemic attack" THEN 1 Else 0 End) as Stroke,
# MAGIC (case when Disease = "Myocardial infarction" THEN 1 Else 0 End) as MI
# MAGIC from cte_death

# COMMAND ----------

# MAGIC %sql
# MAGIC ----First record of Stroke from primary care and HES, table which includes PERSON_ID_DEID and flag for Stroke for each data source
# MAGIC 
# MAGIC create or replace global temp view dp_firstStroke_event AS
# MAGIC 
# MAGIC --- only those that have a first Stroke code
# MAGIC with cte_cases as (
# MAGIC   SELECT *
# MAGIC   from dars_nic_391419_j3w9t_collab.descriptive_StrokeMIevent_firstall
# MAGIC   where Stroke=1
# MAGIC ),
# MAGIC 
# MAGIC ---- first record of any Stroke code from all sources
# MAGIC cte_first as (
# MAGIC select row_number() over (partition by PERSON_ID_DEID, 
# MAGIC                           SOURCE
# MAGIC                           order by DATE desc) as rn,
# MAGIC                     PERSON_ID_DEID,
# MAGIC                     SOURCE,
# MAGIC                     DATE
# MAGIC                from cte_cases
# MAGIC ) 
# MAGIC                
# MAGIC --- eleigable population
# MAGIC SELECT tab2.PERSON_ID_DEID, tab2.SOURCE, tab2.DATE, tab1.NHS_NUMBER_DEID 
# MAGIC from global_temp.dp_skinny tab1
# MAGIC left join cte_first tab2 on tab1.NHS_NUMBER_DEID = tab2.PERSON_ID_DEID
# MAGIC where DATE > '2019-12-31' and rn=1

# COMMAND ----------

# MAGIC %sql
# MAGIC ------ Removing those with previous record of Stroke using descriptive_MI_Stroke_previous
# MAGIC -------fINAL Stroke table
# MAGIC 
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.dp_firststroke_event_table AS
# MAGIC 
# MAGIC with stroke_prev as (
# MAGIC   select distinct(PERSON_ID_DEID) AS ID, Disease
# MAGIC     FROM dars_nic_391419_j3w9t_collab.descriptive_strokemi_previous_table
# MAGIC   where Disease = "Intracerebral haemorrhage" OR Disease = "Ischaemic stroke" OR Disease = "Stroke NOS" OR Disease = "Transient ischaemic attack" OR Disease = "HS stroke" OR Disease = "IS stroke" OR Disease = "NOS stroke" OR Disease = "TIA" 
# MAGIC )
# MAGIC 
# MAGIC SELECT t1.*, t2.ID
# MAGIC FROM global_temp.dp_firstStroke_event t1
# MAGIC     LEFT JOIN stroke_prev t2 ON t1.PERSON_ID_DEID = t2.ID
# MAGIC WHERE t2.ID IS NULL
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC ----First record of MI from primary care and HES, table which includes PERSON_ID_DEID and flag for MI for each data source
# MAGIC 
# MAGIC create or replace global temp view dp_firstmi_event AS
# MAGIC 
# MAGIC --- only those that have a MI code
# MAGIC with cte_cases as (
# MAGIC   SELECT *
# MAGIC   from dars_nic_391419_j3w9t_collab.descriptive_StrokeMIevent_firstall
# MAGIC   where MI=1
# MAGIC ),
# MAGIC 
# MAGIC ---- first record of any MI code from all sources
# MAGIC cte_first as (
# MAGIC select row_number() over (partition by PERSON_ID_DEID, 
# MAGIC                           SOURCE
# MAGIC                           order by DATE desc) as rn,
# MAGIC                     PERSON_ID_DEID,
# MAGIC                     SOURCE,
# MAGIC                     DATE
# MAGIC                from cte_cases
# MAGIC ) 
# MAGIC 
# MAGIC SELECT tab2.PERSON_ID_DEID, tab2.SOURCE, tab2.DATE, tab1.NHS_NUMBER_DEID 
# MAGIC from global_temp.dp_skinny tab1
# MAGIC left join cte_first tab2 on tab1.NHS_NUMBER_DEID = tab2.PERSON_ID_DEID
# MAGIC where DATE > '2019-12-31' and rn=1

# COMMAND ----------

# MAGIC %sql
# MAGIC ------ Removing those with previous record of MI using descriptive_strokemi_previous_table
# MAGIC -------fINAL MI table
# MAGIC 
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.dp_firstmi_event_table AS
# MAGIC 
# MAGIC with mi_prev as (
# MAGIC   select distinct(PERSON_ID_DEID) as ID, Disease
# MAGIC     FROM dars_nic_391419_j3w9t_collab.descriptive_strokemi_previous_table
# MAGIC   where Disease = "Myocardial infarction" OR Disease = "MI" 
# MAGIC )
# MAGIC 
# MAGIC SELECT t1.*, t2.ID
# MAGIC FROM global_temp.dp_firstmi_event t1
# MAGIC     LEFT JOIN MI_prev t2 ON t1.PERSON_ID_DEID = t2.ID
# MAGIC WHERE t2.ID IS NULL
