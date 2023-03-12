from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from settings import configurations
from data import DOC, INDEX_NAME

es = Elasticsearch(
        "elasticsearch:9200",
        http_auth=["elastic", "changeme"],
    )

client = IndicesClient(es)

client.create(index=INDEX_NAME, body=configurations)

def print_index():
    for record in DOC:
        resp=es.index(index=INDEX_NAME, id=record['id'], document=record)
        print("print_index: {}".format(resp['result']))

def print_get():
    resp=es.get(index=INDEX_NAME, id=1)
    print("print_get: {}".format(resp['_source']))

def print_search():
    SELECT_ALL = {"match_all": {}}
    es.indices.refresh(index=INDEX_NAME)
    resp=es.search(index=INDEX_NAME, query=SELECT_ALL, size=5)
    print("print_search_hits: {}".format(resp['hits']['total']['value']))
    for hit in resp['hits']['hits']:
        print("print_search: {}".format(hit["_source"]))

print_index()
print_get()
print_search()

# gets _setting & _mapping info
# res=client.get(index=INDEX_NAME)