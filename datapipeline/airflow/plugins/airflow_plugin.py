from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator

class AmadeusAirflowPlugin(AirflowPlugin):
    name = "amadeus" # Nome da biblioteca
    operators = [TwitterOperator]
