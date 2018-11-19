import requests

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

import json
#r = requests.get('http://localhost:9200') 
i = 1
while i < 5:
    r = requests.get('http://swapi.co/api/people/'+ str(i))
    es.index(index='sw', doc_type='people', id=i, body=json.loads(r.content))
    i=i+1
    print "done"
 
print(i)
print es.get(index='sw', doc_type='people', id=5)


