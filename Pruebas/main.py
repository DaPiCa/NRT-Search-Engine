import logging as lg
import logging.config as lg_conf
import elasticsearch
import time
import json

elastic_search = None


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
    elastic_search_server = f"http://localhost:9200"
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


def search_in_elastic(json):
    _index = json["index"]
    _info = json["info"]
    for field in _info:
        _field = field["field"]
        _value = field["value"]
        if _value == "":
            _value = "*"
        # Query exact match
        _query_suggest = {
            "fuzzy": {
                _field: {
                    "value": _value,
                    "fuzziness": 3,  # Ajustar el valor de fuzziness a 1
                    "max_expansions": 5000,
                    "prefix_length": 0,
                    "transpositions": True,
                }
            }
        }

        _query_predict = {
            "bool": {
                "should": [
                    {
                        "match_phrase_prefix": {
                            _field: {
                                "query": _value,
                                "boost": 15.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        },
                    },
                    {
                        "match_phrase": {
                            _field: {
                                "query": _value,
                                "boost": 3.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        }
                    },
                    {
                        "term": {
                            _field: {
                                "value": _value,
                                "boost": 2.0,  # Puedes ajustar el valor del boost según tus necesidades
                            }
                        }
                    },
                    {
                        "wildcard": {
                            _field: {
                                "value": f"*{_value}*",
                                "boost": 1.0,  # Puedes ajustar el valor del boost según tus necesidades
                            },
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }
        response = elastic_search.search(index=_index, query=_query_predict)
        response_suggest = elastic_search.search(index=_index, query=_query_suggest)

        # lg.debug("Response: %s", response)
        # lg.debug("Response suggest: %s", response_suggest)

        if len(response["hits"]["hits"]) < 0:
            lg.info("No results found")
        else:
            lg.info("Found %s results", len(response["hits"]["hits"]))
            for hit in response["hits"]["hits"]:
                lg.info("Found: %s", hit["_source"][_field])
                lg.info("\tScore: %s", hit["_score"])
                keys = sorted(hit["_source"].keys())
                for key in keys:
                    lg.info("\t\t%s: %s", key, hit["_source"][key])

        # Remove from response_suggest the results that are already in response
        for hit in response["hits"]["hits"]:
            for hit_suggest in response_suggest["hits"]["hits"]:
                if hit["_source"][_field] == hit_suggest["_source"][_field]:
                    response_suggest["hits"]["hits"].remove(hit_suggest)

        if len(response_suggest["hits"]["hits"]) > 0:
            lg.info("--------------------------------------------------------------")
            lg.info("Found %s suggested results", len(response_suggest["hits"]["hits"]))
            for hit in response_suggest["hits"]["hits"]:
                lg.info("Maybe you meant: %s", hit["_source"][_field])
                lg.info("\tScore: %s", hit["_score"])
                keys = sorted(hit["_source"].keys())
                for key in keys:
                    lg.info("\t\t%s: %s", key, hit["_source"][key])


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
    json = {
        "index": "offices",
        "info": [
            {"field": "addressLine1", "value": "Kiocho"},
        ],
    }
    lg.info(
        "Searching on field %s with value %s",
        json["info"][0]["field"],
        json["info"][0]["value"],
    )
    search_in_elastic(json)
