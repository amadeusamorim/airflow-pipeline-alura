# Refino a pesquisa para ajudar na análise, no caso, procuro todas as conversas com o usuário principal (@Flamengo)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, countDistinct, sum, date_format

if __name__ == "__main__": # Por padrão, sempre utilizar
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate() # Cria a sessão e appName é o nome do aplicativo

    tweet = spark.read.json(
        "/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake/silver/twitter_flamengomalvadao/tweet"
    ) # Lendo o df com o caminho do meu dl silver

# No meu Pyspark, listei todos os usuários que fizeram mencao a query e escolhi um user ao acaso
# Estarei buscando todas as mencoes e conversas que esse usuario teve

    torcedor = tweet\
        .where("author_id = '364467247'")\
        .select("author_id", "conversation_id") # Pego o ID,  e as conversas do usuario deste ID

# Join da variavel tweet com todas as mensagens + a mensagem do torcedor de ID repassado na var
    tweet = tweet.alias("tweet")\
        .join(
            torcedor.alias("torcedor"),
            [
                torcedor.author_id != tweet.author_id, # Relacionar conversas de outros usuarios com o torcedor escolhido
                torcedor.conversation_id == tweet.conversation_id 
            ],
            'left')\
        .withColumn(
            "torcedor_conversation", # Coluuca com mensagens relacionadas a conversa com o torcedor e a query
            when(col("flamengomalvadao.conversation_id").isNotNull(), 1).otherwise(0) # Quando nao nula, valor é 1
        ).withColumn( # Saber se houve retorno ao torcedor ou nao
            "reply_torcedor",
            when(col("tweet.in_reply_to_user_id") == '364467247', 1).otherwise(0) # Respondendo o user com o ID repassado e agrupo por dia
        ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"), # Quantas msgs unicas tive naquele dia
            countDistinct("tweet.conversation_id").alias("n_conversation"), # Quantas conversas tive
            sum("torcedor_conversation").alias("torcedor_conversation"), # Soma das conversas
            sum("reply_torcedor").alias("reply_torcedor") # Soma das respostas 
        ).withColumn("weekday", date_format("created_date", "E")) # Agrupo por dia da semana
    
    tweet.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json("/home/amadeus/ama/airflow-pipeline-alura/datapipeline/datalake/gold/twitter_insight_tweet") # Passo o caminho da camada gold

# A intencao é criar uma nova tabela para ser analisada, no caso, irá retornar poucos dados pois um torcedor não interage tanto como uma conta oficial de um time, por exemplo, mas restringi a pesquisa por questao computacional