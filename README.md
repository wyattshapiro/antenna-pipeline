# ATENNA Data Engineering Case Study

## Purpose

Design and build a python pipeline that identifies key subscription events such as sign-ups and cancellations. 

## Data

"In this project, you are given a small data set that contains anonymised transactions with a number of merchants selling digital products, some of which are SVOD (subscription video on demand) services such as Premium plans on Netflix. We ask you to design and build a python pipeline that identifies key subscription events such as sign-up and cancellation. Such results could then be passed downstream to generate aggregated insights regarding subscriptions to each individual service, for example, how many people signed up for Hulu in September 2020."


## Resources
This project has the following key dependencies:

| Dependency Name | Documentation                | Description                                                                            |
|-----------------|------------------------------|----------------------------------------------------------------------------------------|
| Docker        | https://www.docker.com/ | We help developers and development teams build and ship apps |
| Airflow        | https://airflow.apache.org/ | A platform created by the community to programmatically author, schedule and monitor workflows |
| AWS S3        | https://aws.amazon.com/s3/ | Object storage built to store and retrieve any amount of data from anywhere |

## Repo Structure

- docker-compose.yml and Dockerfile: Configuration to deploy Airflow in Docker containers
- scripts/entrypoint.sh: Startup bash script to enable Airflow
- dags/source_a__1__transform_raw_subscriptions.py: Defines the Directed Acyclic Graph
- dags/scripts/source_a__1__transform_raw_subscriptions.py: DAG specific python functions that can be called by PythonOperators
- dags/scripts/dag_util.py: Helper python functions that can be called by PythonOperators
- dags/config/source_a__1__transform_raw_subscriptions.py: Variables specific to the transform_raw_subscriptions DAG
- plugins/operators/: Custom Airflow Operators to download/upload files between S3 and local machine which wraps around the S3 Hook (https://airflow.apache.org/docs/apache-airflow/1.10.9/_modules/airflow/hooks/S3_hook.html#S3Hook)


## Deployment

- Clone the repo locally
- Create .env from sample.env and set variables
- $ docker-compose build
- $ docker-compose up
- Login to local airflow with user and password



