from datetime import datetime
from os.path import join
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
