import requests
import os
import json

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_url():
    query = "FlamengoMalvadao"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    # Passando os campos que quero que retorne do tweet
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    # Passando os campos que quero que retorne do usuário
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    # Passando o intervalo de tempo que quero buscar esses tweets (Máximo de 7 dias)
    filters = "start_time=2022-07-27T00:00:00.00Z&end_time=2022-08-01T00:00:00.00Z" # Z de UTC
    # URL passa a pesquisa que quero com os campos já criados
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Função para paginar
def paginate(url, headers, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}" #Se tiver interação, puxar o next_token
    else:
        full_url =url
    data = connect_to_endpoint(full_url, headers)
    yield data # Passo recursivamente
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, headers, data['meta']['next_token'])


def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    for json_response in paginate(url, headers): # Imprimo as páginas até não ter mais
        print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()