# stock-prediction

### Overview

## Getting Started

### Requirements and Dependencies
Make sure you possess all the following files, and that they are placed in the correct directory.

To be placed in the root directory:
- `token.json`: credentials to access the downloaded twitter data in google sheets
- `neon.json`: parameters for the PostgreSQL database hosted on Neon
- `docker-compose.yaml`: to build the docker container where airflow will run 
- `Dockerfile`
- `requirements.txt`

> Note: `token.json` and `neon.json` must be requested from  separately from the contributors

### Setting up Airflow in Docker 
1. Navigate to the directory where `docker-compose.yaml` is located
2. Initialise the environment 
```sh
docker compose up airflow-init
``` 
3. Build the containers and images. Note that this process will take some time  due to the installation of all the required python packages and dependencies (~ 5-10 mins).

```sh 
docker compose up --build
```

### After Airflow is Set Up
1. After airflow is up and running, navigate to Admin --> Connections --> Click on the `+` icon. Specify your own `Connection Id`, and add a new connection using the parameters provided to you in `neon.json`. 
2. Place the following files into the `/dags` directory, or where you have set airflow to process dags from:
    - `project_DAG.py`
    - `Analysis.py`
    - `Connection.py`
    - `Query.py`
    - `stock_news.py`
    - `stock_price.py`
    - `stock_tweet.py`
3. In `project_DAG.py`, change the variable `CONN_ID` to the one you have created above