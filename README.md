# Airflow ETL to process dataset


## Installation

If this repo is not in linux root directory, may need to configure the following variable in the airflow.config file
```bash
dags_folder = ~/airflow/dags
```

## Start up
Install requirements
```bash
airflow scheduler
```
```bash
airflow webserver
```

## Taskflow
- Extract dataset1 & dataset2 from input folder
- Transform these 2 datasets based on logics provided
- Load data into output folder
