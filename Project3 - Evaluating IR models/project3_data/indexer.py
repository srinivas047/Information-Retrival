import os
# import pysolr
import requests
import json
import urllib.request
import urllib.parse
from urllib.request import urlopen

import subprocess
import sys
import re
from nltk.corpus import stopwords
# import nltk
from nltk.stem import PorterStemmer

try:
    # import pandas as pd
    import pysolr as pysolr
    # import psaw as pw
except ValueError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pysolr'])
    # subprocess.check_call([sys.executable, "-m", "pip", "install", 'psaw'])
# finally:
    # import pandas as pd

# CORE_NAME = "VM25"
AWS_IP = "34.125.10.235"

stop_words = set(stopwords.words('english')) 
def removeStopwords(tokens):
    filtered_sentence = []
    for w in tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    
    return filtered_sentence

#Porter Stemming
ps = PorterStemmer()
def stemming(tokens):
    stemmed_tokens = []
    for w in tokens:
        stemmed_tokens.append(ps.stem(w))
    
    return stemmed_tokens

def processingQueries(query):
    # Pre Processing
    # 1. Removed special characters
    # 2. Removed excess white spaces
    # 3. Whitespace tokenization
    # 4. Removed stopwords
    # 5. Performed Porter Stemming
    # removed_text = re.sub('[^a-zA-Z.\d\s]', ' ', query)
    removing_extra_spaces = re.sub('\s{2,}', ' ', query)
    stripped = removing_extra_spaces.strip()
    tokens = re.split('\s+', stripped)
    # removed_stopwords = removeStopwords(tokens)
    # stemmed_tokens = stemming(removed_stopwords)
    
    return tokens


class Indexer:
    def __init__(self, CORE_NAME):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)
        self.CORE_NAME = CORE_NAME

    def delete_core(self):
        print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=self.CORE_NAME)))


    def create_core(self):
        print(os.system('sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(core=self.CORE_NAME)))

    def do_initial_setup(self):
        self.delete_core()
        self.create_core()    

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data = {
            "add-field": [
                {
                    "name": "lang",
                    "type": "string",
                    "multiValued": False
                }, {
                    "name": "text_de",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "text_en",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "tweet_urls",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "text_ru",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "tweet_hashtags",
                    "type": "string",
                    "multiValued": True
                },
            ]
        }

        print(requests.post(self.solr_url + self.CORE_NAME + "/schema", json=data).json())

if __name__ == "__main__":
    json_data = json.load(open("train.json"))
    i = Indexer('BM25')
    i.do_initial_setup()
    i.create_documents(json_data)

    j = Indexer('VSM')
    j.do_initial_setup()
    j.create_documents(json_data)

    # j = Indexer('VMS')
    # j.do_initial_setup()
