# Real Time Movie Data Pipeline with Cloud Functions and BigQuery


## Overview
This project demonstrates how to build a serverless data pipeline using **Apache Beam** and **Google Cloud Dataflow**. The pipeline reads raw movie data from a **Google Cloud Storage (GCS)** bucket, processes it in real time using Dataflow, and stores the transformed data in **BigQuery**.

## Key Components
1. **Google Cloud Storage (GCS):** Stores raw movie data files.
2. **Apache Beam:** Defines the data pipeline logic for processing and transforming data.
3. **Dataflow:** Executes the Apache Beam pipeline in a fully managed environment.
4. **BigQuery:** Stores the transformed and aggregated data for analysis.

## Project Features
- Real-time data transformation with Apache Beam.
- Integration with GCS and BigQuery.
- Serverless, fully managed execution on Dataflow.


**Data Flow**:
    - Files uploaded to **Google Cloud Storage (GCS)** trigger Pub/Sub notifications.
    - Pub/Sub messages trigger a **Cloud Function** to process the data.
    - Processed data is loaded into a **BigQuery** table for analytics.

## Diagram
Refer to the `diagrams/architecture.png` file for an architectural overview.

### **Prerequisites**
Ensure you have:
1. A **Google Cloud Platform (GCP)** project with billing enabled.
2. **Google Cloud SDK** installed locally and project must me authorized(if not, use following to authorize):
    ```bash
    gcloud auth login
    ```
3. **Python 3.11** installed.
4. Required GCP APIs enabled:
   - **Dataflow API**
   - **BigQuery API**
   - **Cloud Storage API**
5. Your GCP account's default service account must have following permissions:
   - **BigQuery Data Editor**
   - **Storage Admin**

## Steps to Execute

### Step 1: Clone the Repository
1. **Create Topic**:
    ```bash
    git clone https://github.com/aman-shaurya07/Real-Time-Movie-Data-Pipeline-with-Cloud-Functions-and-BigQuery.git
    ```

### Step 2: Set Up GCS Bucket and Notifications
1. **Create a GCS Bucket and create a folder with name "raw"**:
    ```bash
    gcloud storage buckets create gs://<RAW_DATA_BUCKET_NAME> --location=us-central1
    ```
2. **Create a GCS Bucket for temporary data and create a folders with name "temp" and "staging"**:
    ```bash
    gcloud storage buckets create gs://<TEMP_BUCKET_NAME> --location=us-central1
    ```


### Step 3: Create BigQuery Dataset and Table
1. **Create Dataset in BigQuery**:
    ```bash
    bq --location=US mk --dataset YOUR_PROJECT_ID:movie_data
    ```

2. **Create Table for Transformed Data**:
    ```bash
    bq mk --table YOUR_PROJECT_ID:movie_data.transformed_movie_data schema.json
    ```

### Step 4: Upload Dataset to GCS
1. **Uploading Data**:
    ```bash
    gsutil cp dataset/movie_dataset.csv gs://RAW_DATA_BUCKET_NAME/raw/
    ```

### Step 5: Install Dependencies
1. **Command To Install Dependencies**:
    ```bash
    pip install -r pipeline/requirements.txt
    ```

##  Step 6: Deploy the Dataflow Job
1. **Command To Install Dependencies**:
    ```bash
    python3 pipeline/dataflow_pipeline.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://RAW_DATA_BUCKET_NAME/temp \
    --staging_location=gs://TEMP_DATA_BUCKET_NAME/staging \
    --experiments=use_runner_v2
    ```


