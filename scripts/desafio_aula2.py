import urllib.parse
import os
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Functions

# Create a new client and connect to the server
def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_db(client, db_name):
    try: 
        db = client[db_name] 
        print(f"Conectado ao banco de dados: {db_name}") 
        return db 
    except Exception as e: 
        print(f"Erro ao conectar ao banco de dados: {e}") 
    return

def create_connect_collection(db, collection):
    collection = db["produtos2"]
    return collection

#def extract_api_data(url):
#    response = requests.get(url)
#    return response

def extract_api_data(url): 
    response = requests.get(url)
    return response

def insert_data(collection, data):
    docs = collection.insert_many(data)
    return docs


# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as variáveis de ambiente
username = os.getenv('username')
password = os.getenv('password')

# Codifique o nome de usuário e a senha
username_encoded = urllib.parse.quote_plus(username)
password_encoded = urllib.parse.quote_plus(password)

# Use f-strings para criar a URI
uri = f"mongodb+srv://{username_encoded}:{password_encoded}@cluster0.ugrha.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"



client = connect_mongo(uri)

db = create_connect_db(client, 'db_produtos2')

collection = create_connect_collection(db, "produtos2")

api_data = (extract_api_data("https://labdados.com/produtos"))

insert_result = insert_data(collection, api_data.json())

print (len(insert_result.inserted_ids))





