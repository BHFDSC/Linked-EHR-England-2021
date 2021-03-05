# BHF DSC Phenotyping algorithms

Electronic health record phenotyping algorithms for defining diseases, lifestyle risk factors, biomarkers and other concepts related to the manuscript ["Linked electronic health records for research on a nationwide cohort including over 54 million people in England"](https://www.medrxiv.org/content/10.1101/2021.02.22.21252185v2) produced by the CVD-COVID-UK consortium.

You can find more information on the datasets that are used in the CVD-COVID-UK dataset on the
[HDR Gateway](https://web.www.healthdatagateway.org/collection/3975719127757711).

Phenotypes are defined using two clinical terminologies:

* SNOMED-CT used in the [General Practice Extraction Service (GPES) Data for pandemic planning and research (GDPPR)](https://digital.nhs.uk/coronavirus/gpes-data-for-pandemic-planning-and-research/guide-for-analysts-and-users-of-the-data) primary care dataset
* ICD-10 used in hospitalization records from Hospital Episode Statistics (HES) and Office for National Statistics (ONS) mortality

| Phenotype | File name | SNOMED-CT | ICD-10 |
| --------- | ---------- | -------| -------|
| Acute myocardial infarction | AMI | ✔ | Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | |  |
| Obesity                     | obesity | ✔ | Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype |            
| Diabetes                    | diabetes | ✔ | ✔ | 
| Life threatening arrhythmias | arrhythmia |  |  ✔ | 
| Stroke haemmorhagic         | stroke_HS |  ✔ |  Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | | |  |
| Stroke ischaemic            | stroke_IS |  ✔ |  Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | ||  |
| Stroke unspecified          | stroke_NOS |  ✔ |  Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | ||  |
| Stroke SAH                  | stroke_SAH |  ✔ |  Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | ||  |
| Stroke TIA                  | stroke_TIA |  ✔ |  Based on [CALIBER](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext) phenotype | | |  |
| COVID-19 infection          | COVID19 |  ✔ | ✔ |
