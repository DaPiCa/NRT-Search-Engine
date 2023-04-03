from flask import Flask, request, jsonify, Response
import os
import elasticsearch
import logging as lg
import logging.config as lg_conf
import time
import json

app = Flask(__name__)
global es


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/insert", methods=["POST"])
def insert():
    global es
    # Insert data into ElasticSearch
    data = request.get_json()
    data_dict = data["data"]
    for insert in data_dict:
        event = insert["values"]
        # Check if the element is a dict
        if isinstance(event, dict):
            resp = es.index(index=data["table"], body=event)
            lg.debug(f"Response from ElasticSearch: {resp}")
    return jsonify({"status": "success"})


def get_id_from_elastic(old_entry, table):

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {key: value}}
                    for key, value in old_entry.items()
                    if value is not None and value != ""
                ]
            }
        }
    }

    lg.debug(f"Query to ElasticSearch: {query}")
    try:
        resp = es.search(index=table, body=query)
        lg.debug(f"Response from ElasticSearch: {resp}")
        if len(resp["hits"]["hits"]) == 0:
            lg.error("No entry found")
            return None, "No entry found"
        elif len(resp["hits"]["hits"]) > 1:
            lg.error("More than one entry found")
            return None, "More than one entry found"
        else:
            return resp["hits"]["hits"][0]["_id"]
    except Exception as e:
        lg.error(f"Error while getting id: {e}")
        return None, "Error while getting id from ElasticSearch"


@app.route("/modify", methods=["POST"])
def modify():
    data = request.get_json()
    data_dict = data["data"]
    for modification in data_dict:
        old_entry = modification["before"]
        new_entry = modification["after"]
        table = data["table"]
        # Get the id of the entry in ElasticSearch
        id = get_id_from_elastic(old_entry, table)
        if isinstance(id, tuple):
            lg.error(f"Error while getting id: {id}")
            return jsonify(
                {"status": f"Error while getting id from {old_entry}: {id[1]}"}
            )
        # Modify the entry in ElasticSearch
        resp = es.update(index=table, id=id, body={"doc": new_entry})
        lg.debug(f"Response from ElasticSearch: {resp}")
    return jsonify({"status": "success"})


def connect_to_elastic():
    ELASTIC_SEARCH_SERVER = (
        f"http://{os.getenv('ELASTIC_HOST')}:{os.getenv('ELASTIC_PORT')}"
    )
    es = elasticsearch.Elasticsearch(
        ELASTIC_SEARCH_SERVER, use_ssl=False, ca_certs=False, verify_certs=False
    )
    while True:
        try:
            es.info()
            lg.info("Connected to ElasticSearch")
            return es
        except elasticsearch.exceptions.ConnectionError as e:
            lg.error("Connection error: %s", e)
            lg.error("Retrying in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    lg_conf.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
        }
    )
    lg.basicConfig(
        format="%(asctime)s | %(filename)s | %(levelname)s |>> %(message)s",
        level=lg.DEBUG,
    )

    es = connect_to_elastic()
    app.run(host="0.0.0.0", port=os.getenv("API_PORT"), debug=True)
