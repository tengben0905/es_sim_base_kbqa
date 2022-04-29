import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers


es_conf = [{"host": '0.0.0.0', "port": 9400}]

es = Elasticsearch("http://0.0.0.0:9400")


def create_index(index_name):
    global es
    indices = es.indices.get_alias().keys()
    if index_name in indices:
        print('已创建过index')
        return

    settings = {
        "settings": {
            "index": {
                "number_of_shards": "1",
                "number_of_replicas": "0"
            }
        },
        "mappings": {
            "properties": {
                # "id": {
                #     "type": "text"
                # },
                "triple_key": {
                    "type": "text",
                    "analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "index": True
                },
                "triple": {
                    "type": "text"
                }
            }

        }
    }
    # 发送到ES
    es.indices.create(index=index_name, body=json.dumps(settings))
    print('创建完成')


def insert_into_es_bulk(data_json, index_name):
    global es
    actions = []
    for i in data_json:
        action = {
            "_index": index_name,
            "_type": "_doc",
            "_source": {
            }
        }
        # id = i[2]
        # action['_id'] = id
        action['_source'] = {
            "triple_key": i[0],
            "triple": i[1]
        }
        actions.append(action)

    helpers.bulk(es, actions)


def get_from_es():
    es = Elasticsearch()
    res = es.search()
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        # print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
        print(hit["_source"])


def main(index_name):
    # 取数据

    data_list = []
    with open("knowledge/Knowledge_20211215.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            tmp = line.split("\t")
            data_list.append([tmp[0] + " " + tmp[1], tmp[0] + " ||| " + tmp[1] + " ||| " + tmp[2]])

    # 建索引
    create_index(index_name)

    # 存入es
    # insert_into_es(data, index_name)
    insert_into_es_bulk(data_list, index_name)


def delete_index(index_name):
    global es
    res = es.indices.delete(index_name)
    print(res)


if __name__ == '__main__':
    index_name = 'kgclue'
    delete_index(index_name)
    main(index_name)
