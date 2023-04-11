from __future__ import annotations
import json
from textwrap import dedent
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.pipeline.training_pipeline import TrainingPipeline

training_pipeline=TrainingPipeline()

with DAG(
    "gemstone_training_pipeline",
    default_args={"retries": 2},
    description="it is my training pipeline",
    schedule="@weekly",
    start_date=pendulum.datetime(2023, 4, 11, tz="UTC"),
    catchup=False,
    tags=["machine_learning ","classification","gemstone"],
) as dag:
    
    dag.doc_md = __doc__
    
    def data_ingestion(**kwargs):
        ti = kwargs["ti"]
        train_data_path,test_data_path=training_pipeline.start_data_ingestion()
        ti.xcom_push("data_ingestion_artifact", train_data_path,test_data_path)

    def data_transformations(**kwargs):
        ti = kwargs["ti"]
        train_data_path,test_data_path=ti.xcom_pull(task_ids="data_ingestion",key="data_ingestion_artifact")
        train_arr,test_arr=training_pipeline.start_data_transformation(train_data_path,test_data_path)
        ti.xcom_push("data_transformations_artifcat", train_arr,test_arr)

    def model_trainer(**kwargs):
        ti = kwargs["ti"]
        train_arr,test_arr = ti.xcom_pull(task_ids="data_transformations", key="data_transformations_artifcat")
        training_pipeline.start_model_training(train_arr,test_arr)
    

    data_ingestion_task = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion,
    )
    data_ingestion_task.doc_md = dedent(
        """\
    #### Ingestion task
    this task creates a train and test file.
    """
    )

    data_transform_task = PythonOperator(
        task_id="data_transformation",
        python_callable=data_transformations,
    )
    data_transform_task.doc_md = dedent(
        """\
    #### Transformation task
    this task performs the transformation
    """
    )

    model_trainer_task = PythonOperator(
        task_id="load",
        python_callable=model_trainer,
    )
    model_trainer_task.doc_md = dedent(
        """\
    #### model trainer task
    this task perform training
    """
    )
data_ingestion_task >> data_transform_task >> model_trainer_task


