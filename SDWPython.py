# Pipeline de ETL 

# Extração 

sdw2023_api_url = 'https://sdw-2023-prd.up.railway.app'

import pandas as pd

df = pd.read_csv('SDW2023.csv')
user_ids = df['UserID'].tolist()

print(user_ids)

import requests 
import json 

def get_user(id):
    response = requests.get(f'{sdw2023_api_url}/users/{id}')
    return response.json() if response.status_code == 200 else None # status code da API, quando apresenta 200 significa que a operação foi bem sucedida, no caso a API encontrou o ID
                                                                    # solicitado e retornou o status code 200.

users = [user for id in user_ids if (user := get_user(id)) is not None] # operador aurus := permite atribuir em formato de expressão

# print(json.dumps(users, indent=2))

# Até aqui foi o processo de extração de dados da tabela

#########################################################################

# Transformação com a API do ChatGPT


openai_api_key = '' #Adicionar aqui a atribuição da chave de API da openai 

import openai

openai.api_key = openai_api_key # Chave de API da openai 

# função que criou requisição de chat completion que conversa com o chatgpt

def generate_ai_news(user):
    completion = openai.ChatCompletion.create( 
        model="gpt-3.5-turbo",
        messages=[
            {   
                "role": "system", 
                "content": "VocÊ é um especialista em marketing bancário."
            },
            {   
                "role": "user", 
                "content": f"Crie uma mensagem para {user['name']} sobre a importância dos investimentos (máximo de 100 caracteres)"
            }
        ]
    )

    return completion.choices[0].message.content.strip('\"')

for user in users:
    news = generate_ai_news(user)
    print(news)
    user['news'].append({
        "icon": "https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg",
        "description": news
    })


####################################################################################

# Começa a parte de load, carregamento das informações para a API da SDW23

def update_user(user):
    response = requests.put(f"{sdw2023_api_url}/user/{user['id']}", json=user)
    return True if response.status_code == 200 else False

for user in users: 
    success = update_user(user)
    print(f"User{user['name']} updated? {success}!")

