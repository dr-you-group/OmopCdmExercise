##Set .Renviron to hide important information
# install.packages("usethis")
# usethis::edit_r_environ()

## Install required packages##
# install.packages("DBI")
# install.packages("bigrquery")
# install.packages("tidyverse")
# install.packages("lubridate")

###################################Ultimate goal of this exercise#################################
##Show age distribution of patients when they diagnosed with atrial fibrillation first in the DB##
##################################################################################################

# Database
# https://youtu.be/am1X-UmvhZI
# Educational video for database: https://youtu.be/MJsOoA8yM7A
# https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF
# Structure of OMOP-CDM v5.3.1: https://ohdsi.github.io/CommonDataModel/cdm531.html

# GCP -> Marketplace -> search by 'OHDSI'

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
object.size(afPt)/1e6 #550MB
afPt %>% pull(person_id) %>% unique() %>% length() #1,216,938

afUniquePtCnt <- DBI::dbGetQuery(con,
                         sprintf("SELECT COUNT(DISTINCT person_id) AS unique_person_count
                                 FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                 WHERE condition_concept_id =  %s
                                 ;",
                                 afId))
afUniquePtCnt #1,216,938

##The first AF diagnosis record
firstAfRecord <- DBI::dbGetQuery(con,
                           sprintf("SELECT condition_occurrence_id, person_id, condition_concept_id, condition_start_date
                                    FROM
                                       (SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY condition_start_date) AS ordinal
                                       FROM bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence
                                       WHERE condition_concept_id =  %s) AS x
                                   WHERE x.ordinal = 1
                                   ;",
                                   afId))
firstAfRecord %>% nrow() #1,216,938
object.size(firstAfRecord)/1e6 #24MB

##Person who diagnosed with AF
afPerson <- DBI::dbGetQuery(con,
                            sprintf("SELECT person_id, gender_concept_id, year_of_birth
                                    FROM person
                                    WHERE person_id IN (SELECT person_id FROM condition_occurrence WHERE condition_concept_id = %s)
                                    ;",
                                    afId))
nrow(afPerson) #1,216,938

firstAfPerson<- firstAfRecord %>% dplyr::inner_join(afPerson, by = "person_id")

firstAfPerson <- firstAfPerson %>% mutate(age_at_first_af = lubridate::year(condition_start_date)- year_of_birth)

sum(firstAfPerson$age_at_first_af < 0) #check the abnormal value

hist(firstAfPerson$age_at_first_af)
summary(firstAfPerson$age_at_first_af)

afPerson2 <- DBI::dbGetQuery(con,
                            sprintf("SELECT pe.person_id, pe.gender_concept_id, pe.year_of_birth, con.condition_occurrence_id, con.condition_start_date,con.condition_concept_id
                                    FROM person AS pe
                                      JOIN condition_occurrence AS con
                                      ON pe.person_id = con.person_id
                                    WHERE con.condition_concept_id = %s
                                    ;",
                                    afId))

####Question. How many patients did conduct ECG on the date of first AF diagnosis or before?

#Hint: find concept for ECG
# 12-Lead ECG Performed (EM)
ecgConcept <- DBI::dbGetQuery(con,
                             "SELECT *
                              FROM bigquery-public-data.cms_synthetic_patient_data_omop.concept
                              WHERE LOWER(concept_name) = '12-lead ecg performed (em)'
                              AND standard_concept = 'S'
                              AND LOWER(domain_id) = 'observation'
                              ;")
ecgConcept
