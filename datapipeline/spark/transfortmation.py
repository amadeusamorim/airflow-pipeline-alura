from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as f
from os.path import join


def get_tweets_data(df):
    return df\
        .select(
            f.explode("data").alias("tweets")).select(
                "tweets.author_id", 
                "tweets.conversation_id", 
                "tweets.created_at", 
                "tweets.id", 
                "tweets.in_reply_to_user_id", 
                "tweets.public_metrics.*", 
                "tweets.text"
            ) # explode nos campos multilinhas
        
            
def get_users_data(df):
    return df\
        .select(
            f.explode("includes.users").alias("users")
            ).select(
                "users.*"
            )

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)
        
def twitter_transform(spark, src, dest, process_date):
    df = spark.read.json(src)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_dest = join(dest, "{table_name}", f"process_date={process_date}") # nome da tabela e data de processamento do dado exportado (partição)

    export_json(tweet_df, table_dest.format(table_name="tweet"))
    export_json(user_df, table_dest.format(table_name="user"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)    
    parser.add_argument("--process-date", required=True)  
    args = parser.parse_args() # vai criar um dicionário com os parâmetros que recebemos

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate() # Procura sessão ativa

    twitter_transform(spark, args.src, args.dest, args.process_date)