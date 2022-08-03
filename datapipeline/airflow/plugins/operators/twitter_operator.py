from airflow.models import BaseOperator, DAG, TaskInstance
# Importar a apply_defaults pois em uma DAG pode ter parâmetros padrão paa todos os operators
from airflow.utils.decorators import apply_defaults 
from hooks.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join # ajuda a criar o caminho do arquivo


class TwitterOperator(BaseOperator): # Um operador é uma classe

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
            self,
            query, # Puxo os parâmetros do ganho/hook
            file_path, # Indica onde quero salvar meu arquivo
            conn_id = None,
            start_time = None,
            end_time = None,
            *args, **kwargs # Conjunto de argumentos chave-valor entre os métodos
        ): # Toda Classe vai ter o init que recebe os parâmetros
            super().__init__(*args, **kwargs)
            self.query = query
            self.file_path = file_path
            self.conn_id = conn_id
            self.start_time = start_time
            self.end_time = end_time

    def create_parent_folder(self): # Cria as pastas faltantes para os arquivos
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) # Cria a pasta e verifica sub-pastas existentes


    def execute(self, context): # Todos os operadoders terão o método execute, usado para executar a tarefa que precisamos
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time
        )
        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False) # ensure_ascii garante que não se percam os caracteres especiais e emojis
                output_file.write(("\n"))

if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="FlamengoMalvadao",
            file_path=join(
                "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake", # Caminho do meu DL
                "twitter_flamengomalvadao", # Num Data Lake, cada pasta é uma tabela e os arquivos, os campos
                "extract_date={{ ds }}", # partição com a data de extraçao
                "FlamengoMalvadao_{{ ds_nodash }}.json"
                ), # {{}} é um jajna template e o ds vai passar uma string com a data de execução, o no dash tira qualquer traço ou ponto, barras, etc
            task_id="test_run"
        )
        ti = TaskInstance(task=to, execution_date=datetime.now() - timedelta(days=1)) # Instncia da tarefa para o dia anterior
        ti.run()