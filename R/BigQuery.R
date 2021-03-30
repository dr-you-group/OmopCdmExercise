##Set .Renviron to hide important information
# install.packages("usethis")
# usethis::edit_r_environ()

## Install required packages##
# install.packages("DBI")
# install.packages("bigrquery")
# install.packages("tidyverse")

###################################Ultimate goal of this exercise#################################
##Show age distribution of patients when they diagnosed with atrial fibrillation first in the DB##
##################################################################################################

####Connection to the public OMOP-CDM database of CMS synthetic patient data####
con <- DBI::dbConnect(
  drv = bigrquery::bigquery(),
  project = "bigquery-public-data",
  dataset = "cms_synthetic_patient_data_omop",
  billing = Sys.getenv("myProject") ##Your project name
)


####Data exploration####
##List of tables in the DB
DBI::dbListTables(con)

##Head of person table
personSample <- DBI::dbGetQuery(con,
                               "SELECT *
                                FROM bigquery-public-data.cms_synthetic_patient_data_omop.person
                                LIMIT 10;")
personSample
str(personSample)

##How many rows in the person table?
rowNumPerson<- DBI::dbGetQuery(con,
                               "SELECT COUNT(*)
                                FROM bigquery-public-data.cms_synthetic_patient_data_omop.person
                                ;")
rowNumPerson #2,326,856

##Exploration of condition_occurrence table (diagnosis table)
rowNumCondition <- DBI::dbGetQuery(con,
                                   "SELECT COUNT(*)
                                    FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                    ;")
rowNumCondition  #289,182,385

##Find concept of atrial fibrillation
# Find concepts with:
# 1. a name including 'atrial fibrillation'
# 2. standard_concept is 'S'
# 3. domain_id is 'condition'

afConcept <- DBI::dbGetQuery(con,
                             "SELECT *
                              FROM bigquery-public-data.cms_synthetic_patient_data_omop.concept
                              WHERE LOWER(concept_name) LIKE '%atrial fibrillation%'
                              AND standard_concept = 'S'
                              AND LOWER(domain_id) = 'condition'
                              ;")
afConcept$concept_name

##How many atrial fibrillation diagnosis in condition_occurrence table?
library(dplyr)
afId <- afConcept %>% filter(concept_name == "Atrial fibrillation") %>% pull(concept_id)

afCnt <- DBI::dbGetQuery(con,
                         sprintf("SELECT COUNT(*) AS count
                                 FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                 WHERE condition_concept_id =  %s
                                 ;",
                                 afId))

afCnt #5,983,191

##How many patients were diagnosed to have atrial fibrillation?
afPt <- DBI::dbGetQuery(con,
                        sprintf("SELECT *
                                 FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                 WHERE condition_concept_id =  %s
                                 ;",
                                afId))
object.size(afPt)/10^6 #550MB
afPt %>% pull(person_id) %>% unique() %>% length() #1,216,938

afUniquePtCnt <- DBI::dbGetQuery(con,
                         sprintf("SELECT COUNT(DISTINCT person_id) AS unique_person_count
                                 FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                 WHERE condition_concept_id =  %s
                                 ;",
                                 afId))
afUniquePtCnt #1,216,938

##The first AF diagnosis
firstAf <- DBI::dbGetQuery(con,
                           sprintf("SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY condition_start_date) AS ordinal
                                   FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                   WHERE condition_concept_id =  %s
                                   AND ordinal = 1
                                   ;",
                                   afId))