
<h1 align="center">Meteorology ETL pipeline with Airflow, Spark, Docker, and AWS </h1>

## About

This is a project designed to practice my learnings in multiple technologies. In this project I develop a data ingestion pipeline that extracts meteorology data to ultimatley load to a Redshift datawarehouse. I extract data from the [**7Timer**](http://www.7timer.info/doc.php) API which is then ingested into s3. This data is then processed in Spark and loaded to redshift. 

The following is the project architecture:

![Capture](https://user-images.githubusercontent.com/28849195/186055102-50702601-c81f-483c-9bfb-f5cb34498d31.PNG)


## Setup

You will have to create an AWS user for Airflow to interact with the s3 bucket.
The credentials for that user will have to be saved in the [airflow-data/creds/s3](https://github.com/QuinnCummings/Meteorology-ETL_Airflow-Spark-Docker-AWS/tree/main/airflow-data/creds) file:

    [etl-proj]
    aws_access_key_id = 
    aws_secret_access_key = 

To connect to the **Redshift** cluster, you will have to provide your Amazon Redshift database name, host and the rest of the credentials in [**dags/dagRun.py**](https://github.com/QuinnCummings/Meteorology-ETL_Airflow-Spark-Docker-AWS/blob/main/dags/dagRun.py):
    
    dbname = ''
    host = ''
    port = ''
    user = ''
    password = ''
    awsIAMrole = ''
    
Start running Airflow on Docker using the following command:

    docker-compose up -d

You can now access Airflow at **localhost:8080** and run dags

![Capture2](https://user-images.githubusercontent.com/28849195/186057805-605c5959-e726-4170-9773-2c0baf006a58.PNG)
