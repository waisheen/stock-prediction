# Refer to the tag of the image you want to use
FROM apache/airflow:2.7.3

# ADD necessary files from the current directory to the image
ADD requirements.txt .
ADD token.json .

# Install the required dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

# Grant permissions
RUN chmod -R 777 "/home/airflow/.cache"