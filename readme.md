# Data pipeline using airflow

## Propose of the project

Since the sparkify a music streaming company is growing rapidly, the company wants to introduce more automation and monitoring to their data warehouse ETL pipelines.

## About datasets

The songs data & log data are in JSON format that are stored in Amazon S3

## Tech stack

- Python
- Apache Airflow
- Postgres
- AWS redshift
- Notion for project management
- Github for Version control

## Usage manual

- Create connection "aws_credentials" & "redshift" for storing aws credentials & redshift connection string, respectively.
- Create variable named s3_bucket, s3_prefix, region and json_format, which will be used in the program
- Run the airflow server using "airflow standalone command"
- Go to localhost:8080 in browser
- Run dag named sparkify_analysis to perform following task in sequence 
  - Create staging, facts and dimension tables
  - Load data from s3 to staging tables
  - Load data from staging tables to facts and dimension tables
  - Check the quality of data