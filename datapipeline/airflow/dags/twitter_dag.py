from datetime import datetime
from os.path import join
from pathlib import Path

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.amadeus import TwitterOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_flamengomalvadao",
        query="FlamengoMalvadao",
        file_path=join(
                "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake", # Caminho do meu DL
                "twitter_flamengomalvadao", # Num Data Lake, cada pasta é uma tabela e os arquivos, os campos
                "extract_date={{ ds }}", # partição com a data de extraçao
                "FlamengoMalvadao_{{ ds_nodash }}.json"
                )
    )

    twitter_transform = SparkSubmitOperator(
        task_id = "transform_twitter_flamengomalvadao",
        application="/home/amadeus/ama/airflow-pipeline-alura/datapipeline/spark/transfortmation.py",
        name="twitter_transformation", # Nome spark chama o job
        application_args = [
            "--src",
            "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake/bronze/twitter_flamengomalvadao/extract_date=2022-08-02",
            "--dest",
            "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake/silver/twitter_flamengomalvadao",
            "--process-date",
            "{{ ds }}"
        ]
    )