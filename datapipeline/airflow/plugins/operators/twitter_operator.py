from airflow.models import BaseOperator, DAG, TaskInstance
# Importar a apply_defaults pois em uma DAG pode ter parâmetros padrão paa todos os operators
from airflow.utils.decorators import apply_defaults 
from hooks.twitter_hook import TwitterHook
import json
from datetime import datetime


class TwitterOperator(BaseOperator): # Um operador é uma classe

    @apply_defaults
    def __init__(
            self,
            query, # Puxo os parâmetros do ganho/hook
            conn_id = None,
            start_time = None,
            end_time = None,
            *args, **kwargs # Conjunto de argumentos chave-valor entre os métodos
        ): # Toda Classe vai ter o init que recebe os parâmetros
            super().__init__(*args, **kwargs)
            self.query = query
            self.conn_id = conn_id
            self.start_time = start_time
            self.end_time = end_time


    def execute(self, context): # Todos os operadoders terão o método execute, usado para executar a tarefa que precisamos
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time
        )
        for pg in hook.run():
            print(json.dumps(pg, indent=4, sort_keys=True))

if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="FlamengoMalvadao", task_id="test_run"
        )
        ti = TaskInstance(task=to, execution_date=datetime.now()) # Instncia da tarefa
        to.execute(ti.get_template_context()) 