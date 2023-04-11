## End To End ML Project

### created a environment
```
conda create -p venv python==3.8

conda activate venv/
```
### Install all necessary libraries
```
pip install -r requirements.txt
```

#command for building a docker image

```
docker build -t gemstone:latest
```

# this is for linux

```
$(pwd)/airflow/dag
```

# this is for windows

```
%cd%\airflow\dags:/app/airflow/dags 
```
# for running the container

```
docker run -p 8080:8080 -v %cd%\airflow\dags:/app/airflow/dags gemstone:latest
```