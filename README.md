
# MLOps Implementation with Apache Airflow
This README provides an overview of the MLOps implementation with Apache Airflow, guiding users on setting up, executing, and managing the ETL pipeline efficiently.

Objective:
Implement Apache Airflow to automate data extraction, transformation, and version-controlled storage.

# Tasks:

1. Data Extraction:
Utilize Dawn and BBC as data sources.
Extract links from the landing page.
Extract titles and descriptions from articles displayed on their homepages.

2. Data Transformation:
Preprocess the extracted text data, ensuring to clean and format it appropriately for further analysis.

3. Data Storage and Version Control:
Store the processed data on Google Drive.
Implement Data Version Control (DVC) to track versions of the data accurately. Ensure each version is recorded as changes are made.
Version metadata against each dvc push to the GitHub repo.

4. Apache Airflow DAG Development:

Write an Airflow DAG to automate the processes of extraction, transformation, and storage.
Ensure the DAG handles task dependencies and error management effectively.

Setup Instructions:
Install Apache Airflow and required Python libraries.
Clone the repository to your local machine.
Configure DVC and Git remotes for version control.
Update Airflow configuration to point to the DAG script.
Start the Airflow scheduler and web server.
Access the Airflow UI to monitor and manage the DAG execution.

Usage:
Ensure proper setup and configuration before executing the Airflow DAG.
Monitor DAG execution and logs in the Airflow UI.
Check the /data directory for processed data files.
Use DVC and Git commands for versioning and collaboration.

Contributors:
Muhammad Kazim 
