import logging as lg
import logging.config as lg_conf
import os
import time

import elasticsearch  # pylint: disable=import-error
from flask import Flask, Response, jsonify, request  # pylint: disable=import-error
from flask_cors import CORS  # pylint: disable=import-error

app = Flask(__name__)
CORS(app)

elastic_search = None  # pylint: disable=invalid-name


@app.route("/getindexes", methods=["GET"])
def get_indexes_for_front():
    """
    Endpoint that retrieves a list of all non-system ElasticSearch indexes and returns
    them as a JSON response to the front-end.

    Returns:
        A JSON response containing a list of all non-system ElasticSearch indexes in
        alphabetical order. If there is an
        error connecting to ElasticSearch, a JSON response containing an error message
        is returned instead.
    """
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
    return jsonify({"status": "ElasticSearch is not connected"})


@app.route("/getfields", methods=["POST"])
def get_fields_from_index():
    """
    Retrieves a list of all the field names in the specified ElasticSearch index
    and returns them in alphabetical order as a JSON response.

    Args:
        None

    Returns:
        A JSON object with a single key "fields" whose value is a sorted list of
        all the field names in the specified ElasticSearch index.

        If the ElasticSearch connection is not available, returns a JSON object with
        a single key "status" whose value is the string "ElasticSearch is not connected".

        If the specified index is not found in ElasticSearch, returns a JSON object with
        a single key "status" whose value is the string "Index not found".
    """
    data = request.get_json()
    _index = data["index"]
    lg.debug("Received request to get fields from index %s", _index)
    if elastic_search is not None:
        try:
            # Return all fields in the index
            response = {
                "fields": sorted(
                    list(
                        elastic_search.indices.get_mapping(index=_index)[_index][
                            "mappings"
                        ]["properties"].keys()
                    )
                )
            }
            lg.debug("Response to get fields from index %s: %s", _index, response)
            return jsonify(response)
        except elasticsearch.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)
            return jsonify({"status": "ElasticSearch is not connected"})
        except elasticsearch.exceptions.NotFoundError as error:
            lg.error("Index not found: %s", error)
            return jsonify({"status": "Index not found"})
    return jsonify({"status": "ElasticSearch is not connected"})


@app.route("/search", methods=["POST"])
def search():
    data = request.get_json()
    _index = data["index"]
    _field = data["field"]
    _query_text = data["query"]
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            _field: {
                                "query": _query_text,
                                "boost": 1.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        }
                    },
                    {
                        "term": {
                            _field: {
                                "value": _query_text,
                                "boost": 10.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        }
                    },
                    {
                        "match_phrase_prefix": {
                            _field: {
                                "query": _query_text,
                                "boost": 1.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        },
                    },
                ],
                "minimum_should_match": 1,
            }
        }
    }
    response = elastic_search.search(index=_index, body=query)
    lg.debug("Response from ElasticSearch: %s", response)
    return jsonify(response["hits"]["hits"])


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
    app.run(host="0.0.0.0", port=os.getenv("API_USER_PORT"), debug=True)  # nosec
