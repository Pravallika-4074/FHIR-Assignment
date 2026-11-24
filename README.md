FHIR API Data Ingestion & Analytics Assignment
Overview
This project implements an end-to-end Medallion Architecture in Microsoft Fabric for ingesting and analyzing FHIR healthcare data.

Resources Processed:

Patient
Encounter
Observation
Condition

Architecture : 

FHIR API 
↓ 
Raw Layer (JSON Files) 
↓ 
Bronze Layer (Delta Tables) 
↓ 
Silver Layer (Cleaned + SCD Type 2) 
↓ 
Gold Layer (Analytics Views) 
↓ 
Reporting / Power BI

Medallion Layers
Raw Layer
Stores original API responses in JSON format.

Path Structure:
Files/raw/{resource}/{date}/{run_id}/

Bronze Layer
Stores raw ingested records in Delta format along with ingestion metadata.

Metadata Columns:

run_id
extraction_timestamp
api_url_or_params
api_called_at
data_saved_at
raw_file_path
Silver Layer
Performs:

Deduplication
Incremental processing
Slowly Changing Dimension Type 2 (SCD2)
Gold Layer
Creates curated reporting views:

vw_gold_patient
vw_gold_encounter
vw_gold_observation
vw_gold_condition
Incremental Loading
The solution uses FHIR _lastUpdated filtering and watermark tracking through the ingestion_state table.

This ensures only new or modified records are processed in subsequent executions.

Slowly Changing Dimension (SCD Type 2)
Historical changes are tracked using:

valid_from
valid_to
is_current
resource_content_hash
Previous versions of records are preserved for auditing and historical analysis.

Audit & Traceability
Audit tables:

ingestion_log
ingestion_state
Tracked Information:

API call timestamp
Data save timestamp
Request URL
Processing status
Record counts
Errors
Data Pipeline
A Microsoft Fabric Data Pipeline orchestrates notebook execution and manages:

Data Ingestion
Metadata Logging
Bronze Processing
Silver Processing
Gold View Creation
Repository Structure
FHIR-Assignment/

├── notebooks/
│   └── 01_FHIR_Ingestion.py
│
├── config/
│   └── config.json
│
├── docs/
│
├── sql/
│
├── README.md
├── requirements.txt
└── .gitignore
Technologies Used
Microsoft Fabric
Spark Notebooks
Delta Lake
Python
GitHub
FHIR API