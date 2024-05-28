# Big Data Engineering Project on Azure
## Project Overview

In this Big Data Engineering project I leverages Azure, Apache Airflow, Apache Spark, DevOps, and Power BI to ingest data from two sources into an Azure Data Lake. Post-ingestion, data transformation and machine learning processes will be performed, with results stored back in the Data Lake. Finally, Azure Synapse and Power BI will be used to generate and visualize reports based on the processed data.Additionally, Azure Key Vault will securely store sensitive information for Databricks mount notebooks.

![Project](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/47e6a869-379b-4201-aecb-2fa1baa7aba1)
## Data Overview
### Data Background
The dataset originates from StackOverflow, capturing daily posts, post types, and user information. 
The dataset's dates have been updated to reflect current dates, ensuring the posts data corresponds to today's date.
![c60ad6c5-1b2a-4d86-9c12-ea9af55064bd](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/e1cefece-5434-4a3e-a82e-b4094180de07)

#### Dataset Schema
The dataset consists of three tables stored in two locations:

RDS: The Users and PostTypes tables are stored in an AWS RDS Postgres database, updated weekly using SCD type 1 (overwrite old records).
Azure Storage Blob: Daily Posts data files in Parquet format.
![posts_files](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/55b8b42b-f1bf-48b3-9171-48fc21cba414)
## Business Requirements
##### Data Lake 
- Create an Azure Data Lake with a hierarchical namespace.
- Use Azure Data Factory (ADF) for data ingestion and processing.
- Ingest Users and PostTypes tables from AWS RDS Postgres to the Data Lake weekly.
- Ingest daily Posts data from Azure Blob container into the Data Lake.
#### Machine Learning Process 
- Create a Databricks notebook to process and clean data, run a machine learning model using Apache Spark, and store results in the Data Lake.
- This machine learning model is to read the posts' text in the Posts files, classify what topic each post is about, and use Spark to output a file listing all the topics for today, ordered by their frequency.
- Use Azure Key Vault to securely store credentials and other sensitive information for Databricks mount notebooks.
####  Chart and Visualization 
- Create a chart in Azure Synapse based on the machine learning output to display the top 10 topics of the day.
- Use Power BI to visualize the data and provide interactive reports and dashboards.
## Project Infrastructure
#### Azure Components
- Azure Data Lake: Store ingested data, machine learning input/output, and processed data.
- Azure Data Factory: Orchestrate data ingestion, transformation, and machine learning processes.
- Azure Synapse: Analyze data and generate BI reports.
- Apache Airflow: Manage and schedule data workflows.
- Apache Spark: Perform data transformation and run machine learning models.
- Power BI: Create interactive visualizations and dashboards.
- Azure Key Vault: Securely store secrets, keys, and certificates.
#### DevOps Integration
Implement DevOps practices to collaborate in Azure Data Factory (ADF).
## Data Ingestion

- Connect to AWS RDS:
Use ADF and Airflow to connect to the stack database and raw_st schema.
Transfer Users and PostTypes tables to the Data Lake (weekly).
![Screenshot 2024-05-16 145216](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/fb9a3971-b573-4e26-8353-c3755a8bacfe)
- Connect to Azure Blob Storage:
Use ADF to copy daily Posts data files from the Azure Blob container to the Data Lake.
Move copied Posts data files to an Archive folder in the Data Lake and delete them from the destination folder to prepare for new data.
![Screenshot 2024-05-16 150018](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/d691499a-1c6c-41b9-ba3a-181f78688e96)
![Screenshot 2024-05-09 162509](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/b474d1a5-8485-4bea-882e-3a53b4a272b6)

- Use Apache Airflow to manage and monitor this pipeline in ADF.
![Screenshot 2024-05-20 233234](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/ff453cca-da6a-4f9e-abea-339943e576af)
## Data Transformation

- Perform data transformation in an ADF pipeline.
- Use a Databricks notebook with Apache Spark for data cleaning, transformation, and running the machine learning model.

- Store the machine learning output in the Data Lake for further analysis.

- Securely store Databricks mount notebook credentials in Azure Key Vault.
![Screenshot 2024-05-16 145343](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/fc4b63af-a0f3-473c-9f85-723adf924acd)
##  Data Visualization

- Use Azure Synapse to connect to the Data Lake.
- Generate a chart displaying the top 10 topics of the day based on the machine learning model output.
![SQL script 1](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/f5d61258-072d-408f-8ee9-4a785f7d6690)
- Utilize Power BI to create interactive reports and dashboards for deeper insights and visualization of the data.
![5924876135106199796_121](https://github.com/Anqa-H/End-to-end-big-data-pipeline-for-StackOverflow-posts-to-classify-topics-/assets/80011409/98fa64a2-28d0-4dd8-8d82-904e58d95543)
