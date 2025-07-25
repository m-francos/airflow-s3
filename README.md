# Apache Airflow & Spark

This repository is intended for orchestrating Spark applications using the Apache Airflow tool with AWS S3 storage.

## Table of Contents

1. [Technologies](#technologies)
2. [Install and Run](#install-and-run)
3. [S3 Configuration](#s3-configuration)
4. [Apache Airflow Webserver](#apache-airflow-webserver)
5. [Databricks Lakehouse Architecture](#databricks-lakehouse-architecture)

## 1. Technologies

A list of technologies used within the project:

- [Python](https://www.python.org): Version 3.12
- [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html): Version 3.5.3
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html): Version 2.10.2
- [Amazon S3](https://aws.amazon.com/s3/): Cloud storage for data lake

## 2. Install and Run

```bash
# Clone this repo
$ git clone https://github.com/m-francos/airflow-s3.git
$ cd airflows3
```

### Windows

```bash
# Create a virtual environment
$ python -m venv airflow_venv

# Activate your virtual environment
$ airflow_venv\Scripts\activate

# Install requirements
$ pip install -r requirements.txt
```

### MacOS & Linux

```bash
# Create a virtual environment
$ python3 -m venv airflow_venv

# Activate your virtual environment
$ source airflow_venv/bin/activate

# Install requirements
$ pip install -r requirements.txt
```

### Config DAG Folder

To ensure that Airflow recognizes the DAG developed for this project, you can follow these steps. However, remember that this is only a temporary transformation, so don’t forget to change the DAG path back to the original version.

```bash
# Open the terminal and go to the root path
# Go to airflow folder
$ cd airflow

# Open airflow config file
$ nano airflow.cfg

# Add a comment # on the existing `dags_folder` setting, then add the path to the clone repository

# dags_folder = /home/youruser/airflow/dags
dags_folder = /home/your/path/to/this/repo/lakehouse/airflow/dags
```

### Run Apache Airflow

*Ensure that Apache Airflow is installed on your machine.*

Initialize Airflow:
```bash
airflow db init
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin
```

Run Airflow in **two terminals**:
```bash
# Terminal 1
airflow scheduler

# Terminal 2
airflow webserver -p 8080
```

Or use **standalone mode**:
```bash
airflow standalone
```

## 3. S3 Configuration

This project uses AWS S3 for data storage.  
Fill the a `.env` file in the project root with:

```
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
```

Also, when Airflow is running (http://localhost:8080), before triggering the DAG, go to:

**Admin > Connections > aws_default**  
- Fill in the **Login** field with your `AWS_ACCESS_KEY_ID`
- Fill in the **Password** field with your `AWS_SECRET_ACCESS_KEY`
- Save.

## 4. Apache Airflow Webserver

Webserver will start at: `http://localhost:8080`

From here, you can access the address above in your browser and log in.

The first configuration of the web server should be to change the host in Airflow. To do this, go to **Admin > Connections** > Search for **"spark_default"** > Change the **"Host"** field from **"yarn"** to **"local"** and save.

Next, configure the AWS keys by going to Admin > Connections > Search for "aws_default" > Fill the "Login" field with your AWS_ACCESS_KEY_ID and the "Password" field with your AWS_SECRET_ACCESS_KEY, then save.

Then, you can go to the **"Search Dags"** field and search for **"lakehouse_pipeline"**. Click on the search result, and you will have access to the interface related to the created Airflow instance. You can execute it by clicking the **"Trigger DAG"** button in the upper right corner of the screen, where you can observe the execution order and whether the Spark applications were successful or not. You can verify this by looking in the repository for each of the bronze, silver, and gold folders, where a subfolder called `parquet` will be created.

## 5. Databricks Lakehouse Architecture

The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse, aimed at progressively enhancing the structure and quality of data as it flows through each layer of the architecture. The layers include Bronze (raw data), Silver (cleaned and transformed data), and Gold (data ready for analysis).
