from datetime import datetime
from os.path import join
from pathlib import Path

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.amadeus import TwitterOperator
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "airflow", # Nome do responsável pelo DAG
    "depends_on_past": False, # Vai depender de uma inst. anterior ou não (Nosso caso nao precisa da data anterior)
    "start_date": days_ago(6) # Quando iniciar a tarefa? (Nosso caso, seis dias atrás da data que eu iniciar a execucao)
}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z" # Timestamp aceito pelo Twitter

with DAG(
    dag_id="twitter_dag", 
    default_args=ARGS,
    schedule_interval="0 9 * * *", # Frequencia da execucao / Cron min hora diames meses semanas (todo dia 9 da manha)
    max_active_runs=1 # Executa uma instancia por vez
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_flamengomalvadao",
        query="FlamengoMalvadao",
        file_path=join(
                "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake", # Caminho do meu DL
                "twitter_flamengomalvadao", # Num Data Lake, cada pasta é uma tabela e os arquivos, os campos
                "extract_date={{ ds }}", # partição com a data de extraçao
                "FlamengoMalvadao_{{ ds_nodash }}.json"
                ),
        start_time = (
            "{{" 
            f"execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}" # Timestamp do momento de execucao adaptado para str repassada na variavel TIMESTAMP_FORMAT
        ),
        end_time = (
            "{{" 
            f"next_execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}" # Da a proxima data de execucao ate a proxima data de exec
        ),
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

    twitter_operator >> twitter_transform # Conecto o primeiro operador ao segundo (em sequencia)