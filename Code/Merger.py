import json

from elasticsearch7.client import Elasticsearch
from elasticsearch7.helpers.actions import bulk


index_name = 'hw3'
cloud_id = 'My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NjBlOWE4NjFkZDk0NzEyODk2YzE4ZDJhMTA4ZDg5MiQwMjIyMWIyNjJiNDI0M2EwODFhYTNkMjFjMDMzYmI4NQ=='
es = Elasticsearch(request_timeout=10000, cloud_id=cloud_id, http_auth=('elastic', 'xb9aGsKcVInt2lNrltTfDT2q'))


def create_index():
    index_json = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "max_result_window": 100000,
            "analysis": {
                "analyzer": {
                    "stopped": {
                        "type": "custom",
                        "tokenizer": "standard"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "stopped",
                    "index_options": "positions",
                },
                "inlinks": {
                    "type": "text"
                },
                "outlinks": {
                    "type": "text"
                },
                "author": {
                    "type": "text"
                }
            }
        }
    }
    es.indices.create(index=index_name, body=index_json)


def merge(url, body, inlinks, outlinks, author):
    if es.exists(index=index_name, id=url):
        existing = es.get(index=index_name, id=url)['_source']
        existing_inlinks = existing.get('inlinks', [])
        updated_inlinks = list(set(existing_inlinks + inlinks))
        existing_author = str(existing.get('author', ''))
        updated_author = existing_author + ", " + "Julie Kim"
        existing_content = existing.get('content')
        update_body = {
            "doc": {
                "content": existing_content,
                "inlinks": updated_inlinks,
                "outlinks": outlinks,
                "author": updated_author
            }
        }
        es.update(index=index_name, body=update_body, id=url)
    else:
        insert_body = {
            "content": body,
            "inlinks": inlinks,
            "outlinks": outlinks,
            "author": author
        }
        es.index(index=index_name, document=insert_body, id=url)


num = 1
file = open('C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/index_0.json')
dict1 = json.load(file)
for url in dict1.keys():
    merge(url, dict1[url]['content'], dict1[url]['inlinks'], dict1[url]['outlinks'], dict1[url]['author'])
    print(num)
    num += 1

