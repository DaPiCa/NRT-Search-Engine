import logging as lg
import logging.config as lg_conf
import os
import time

import elasticsearch  # pylint: disable=import-error
from flask import Flask, Response, jsonify, request  # pylint: disable=import-error
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

elastic_search = None  # pylint: disable=invalid-name


@app.route("/getindexes", methods=["GET"])
def get_indexes_for_front():
    # List all indexes in ElasticSearch and return them to the front-end
    if elastic_search is not None:
        try:
            # Return all indexes that do not start with a dot and order them alphabetically
            return jsonify(
                {
                    "indexes": sorted(
                        [
                            index
                            for index in elastic_search.indices.get_alias().keys()
                            if not index.startswith(".")
                        ]
                    )
                }
            )

        except elasticsearch.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)
            return jsonify({"status": "ElasticSearch is not connected"})


@app.route("/getfields", methods=["POST"])
def get_fields_from_index():
    data = request.get_json()
    _index = data["index"]
    if elastic_search is not None:
        try:
            # Return all fields in the index
            return jsonify(
                {
                    "fields": sorted(
                        [
                            field
                            for field in elastic_search.indices.get_mapping(
                                index=_index
                            )[_index]["mappings"]["properties"].keys()
                        ]
                    )
                }
            )
        except elasticsearch.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)
            return jsonify({"status": "ElasticSearch is not connected"})
        except elasticsearch.exceptions.NotFoundError as error:
            lg.error("Index not found: %s", error)
            return jsonify({"status": "Index not found"})


@app.route("/insert", methods=["POST"])
def insert_data() -> Response:
    """
    A Flask route that accepts POST requests to insert data into Elasticsearch.

    The function expects a JSON payload with two keys - 'table' and 'data'.
    The 'table' key contains the name of the Elasticsearch index, while the 'data'
    key is a list of dictionary objects, each representing a document to be inserted.

    The function inserts each document in the 'data' list into Elasticsearch using the
    Elasticsearch Python client library.

    Returns:
        Response: A Flask Response object with a JSON payload containing a status message.
    """
    data = request.get_json()
    data_dict = data["data"]
    for insert in data_dict:
        event = insert["values"]
        if isinstance(event, dict):
            try:
                if elastic_search is None:
                    lg.error("ElasticSearch is not connected")
                    return jsonify({"status": "ElasticSearch is not connected"})
                resp = elastic_search.index(index=data["table"], body=event)
                lg.debug("Response from ElasticSearch: %s", resp)
            except elasticsearch.exceptions.ConnectionError as error:
                lg.error("Connection error: %s", error)
    return jsonify({"status": "success"})


def get_id_from_elastic(old_entry: dict, table: str) -> tuple:
    """
    A helper function to retrieve the document ID of an Elasticsearch document
    matching the given old_entry.

    The function constructs an Elasticsearch query to match the 'old_entry' values
    against the documents in the Elasticsearch index specified by 'table' parameter.

    If a single matching document is found, the function returns the document ID.
    If no matching document is found, the function returns None and an error message.
    If more than one matching documents are found, the function returns None and an error message.

    Args:
        old_entry (dict): A dictionary object representing the values of the document
        to be modified.
        table (str): A string representing the name of the Elasticsearch index to search.

    Returns:
        tuple: A tuple containing either the document ID or None, and an error message (if any).
    """
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

    lg.debug("Query to table %s in ElasticSearch: %s", table, query)
    try:
        if elastic_search is None:
            lg.error("ElasticSearch is not connected")
            return jsonify({"status": "ElasticSearch is not connected"})
        resp = elastic_search.search(index=table, body=query)
        lg.debug("Response from ElasticSearch: %s", resp)
        if len(resp["hits"]["hits"]) == 0:
            lg.error("No entry found")
            return None, "No entry found"
        if len(resp["hits"]["hits"]) > 1:
            lg.error("More than one entry found")
            return None, "More than one entry found"
        return resp["hits"]["hits"][0]["_id"]
    except Exception as error:
        lg.error("Error while getting id: %s", error)
        return None, "Error while getting id from ElasticSearch"


@app.route("/modify", methods=["POST"])
def modify_data():
    """
    A Flask route that accepts POST requests to modify data from Elasticsearch.

    The function expects a JSON payload with two keys - 'table' and 'data'.
    The 'table' key contains the name of the Elasticsearch index, while the 'data'
    key is a list of modifications. Each modification is a dictionary object with two keys.

    Returns:
        Response: A Flask Response object with a JSON payload containing a status message.
    """
    data = request.get_json()
    data_dict = data["data"]
    for modification in data_dict:
        old_entry = modification["before"]
        new_entry = modification["after"]
        table = data["table"]
        # Get the id of the entry in ElasticSearch
        _id = get_id_from_elastic(old_entry, table)
        if isinstance(_id, tuple):
            lg.error("Error while getting id: %s", _id[1])
            return jsonify(
                {"status": f"Error while getting id from {old_entry}: {_id[1]}"}
            )
        # Modify the entry in ElasticSearch
        if elastic_search is None:
            lg.error("ElasticSearch is not connected")
            return jsonify({"status": "ElasticSearch is not connected"})
        try:
            resp = elastic_search.update(index=table, id=_id, body={"doc": new_entry})
            lg.debug("Response from ElasticSearch: %s", resp)
            return jsonify({"status": "success"})
        except Exception as error:
            lg.error("Error while modifying entry: %s", error)
            return jsonify(
                {
                    "status": f"Error while modifying entry from {old_entry} to {new_entry}: {error}"
                }
            )


@app.route("/delete", methods=["POST"])
def delete_data():
    lg.debug("Delete request received: %s", request)
    data = request.get_json()
    data_dict = data["data"]
    for delete in data_dict:
        event = delete["values"]
        table = data["table"]
        # Get the id of the entry in ElasticSearch
        _id = get_id_from_elastic(event, table)
        if isinstance(_id, tuple):
            lg.error("Error while getting id: %s", _id[1])
            return jsonify({"status": f"Error while getting id from {event}: {_id[1]}"})
        # Modify the entry in ElasticSearch
        if elastic_search is None:
            lg.error("ElasticSearch is not connected")
            return jsonify({"status": "ElasticSearch is not connected"})
        try:
            resp = elastic_search.delete(index=table, id=_id)
            lg.debug("Response from ElasticSearch: %s", resp)
            return jsonify({"status": "success"})
        except Exception as error:
            lg.error("Error while deleting entry: %s", error)
            return jsonify(
                {"status": f"Error while deleting entry from {event}: {error}"}
            )


def connect_to_elastic() -> None:
    """
    Connects to an ElasticSearch instance using the environment variables for the host and port.

    Returns:
        es (elasticsearch.Elasticsearch): An instance of the Elasticsearch client.

    Raises:
        elasticsearch.exceptions.ConnectionError: If the connection to the
        ElasticSearch server cannot be established.
    """
    global elastic_search  # pylint: disable=global-statement, invalid-name
    elastic_search_server = (
        f"http://{os.getenv('ELASTIC_HOST')}:{os.getenv('ELASTIC_PORT')}"
    )
    elastic_search = elasticsearch.Elasticsearch(
        elastic_search_server, use_ssl=False, ca_certs=False, verify_certs=False
    )
    while True:
        try:
            elastic_search.info()
            lg.info("Connected to ElasticSearch")
            break
        except elasticsearch.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)
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

    connect_to_elastic()
    app.run(host="0.0.0.0", port=os.getenv("API_PORT"), debug=True)  # nosec
