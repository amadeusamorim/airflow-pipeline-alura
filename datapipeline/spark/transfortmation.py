from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate() # Procura sessão ativa
    
    df = spark.read.json(
        "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake/twitter_flamengomalvadao"
    )

    df.printSchema()
    df.show()