# stock-prediction

### Setting up the docker containers 
- Obtain the `token.json` credentials to access the twitter data in google drive
- Place `docker-compose.yaml`, `Dockerfile`, `requirements.txt` and `token.json` in the same directory
- Run `docker compose up --build` to build the containers and images

### Create and connect to a database
- The .yaml file above creates a container which runs the PostgreSQL service 
- To add the connection in airflow:
    - Connection Type: `Postgres`
    - Host: `host.docker.internal`
    - Database: `is3107`
    - Login: `is3107`
    - Password: `is3107`
    - Port: `5433`
- You can also use your own localhost/ other external PostgreSQL connections. However, make sure to add it into airflow

### Executing the DAG
- Place the following files into the `/dags` directory, or where you have set airflow to process dags from:
    - `project_DAG.py`
    - `stock_db.py`
    - `stock_news.py`
    - `stock_price.py`
    - `stock_tweet.py`
- In `project_DAG.py`, change the variable `CONN_ID` to the one you have created above