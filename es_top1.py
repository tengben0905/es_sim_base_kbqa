import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es_conf = [{"host": '0.0.0.0', "port": 9400}]

es = Elasticsearch("http://0.0.0.0:9400")


def get_res_from_es(query):
    global es
    query_body = {
                    "query": {
                        "match": {
                            "triple_key": query
                            }
                        },
                    "size": 1
    }

    index_name = "kgclue"
    docs = es.search(body=query_body, index=index_name)["hits"]["hits"] if query_body else []

    return docs


if __name__ == '__main__':
    # test_file_object = open("../datasets/test_public.json", 'r', encoding='utf-8')
    test_file_object = open("../datasets/test.json", 'r', encoding='utf-8')
    test_lines = test_file_object.readlines()
    # target_object = open("test_public_prediction.json", 'w', encoding='utf-8')
    target_object = open("kgclue_predict_es_sim_base.json", 'w', encoding='utf-8')

    res_jsons = []
    for i, line in enumerate(test_lines):
        question = json.loads(line.strip())["question"]
        res = get_res_from_es(question)
        res = res[0]["_source"]["triple"].replace("\n", "")

        json_string = {"id": i, "answer": res}
        json_string = json.dumps(json_string, ensure_ascii=False)
        target_object.write(json_string + "\n")