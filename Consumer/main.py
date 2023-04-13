# pylint: disable=import-error, line-too-long

import calendar
import threading
import json
import logging as lg
import logging.config as lg_conf
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import NoBrokersAvailable, UnrecognizedBrokerVersion
import requests


def insert(msg: ConsumerRecord) -> None:
    """Function that receives a ConsumerRecord object and logs an insert message.

    Args:
        msg (ConsumerRecord): ConsumerRecord object containing the received message.

    Returns:
        None.
    """
    lg.info("Insert: %s", msg.value.decode("utf-8"))
    lg.debug(
        "Time between insert and consume: %d seconds",
        calendar.timegm(time.gmtime()) - msg.timestamp,
    )
    try:
        post = requests.post(
            f"http://{os.getenv('API_ELASTIC_HOST')}:{os.getenv('API_ELASTIC_PORT')}/insert",
            json=json.loads(msg.value.decode("utf-8")),
            timeout=5,
        )
        lg.debug("Response from API: %s", post.text)
    except requests.exceptions.ConnectionError as error:
        lg.error("Connection error: %s", error)


def modification(msg: ConsumerRecord) -> None:
    """Function that receives a ConsumerRecord object and logs a modification message.

    Args:
        msg (ConsumerRecord): ConsumerRecord object containing the received message.

    Returns:
        None.
    """
    if json.loads(msg.value.decode("utf-8"))["type"] == "insert":
        insert(msg)
    else:
        lg.info("Modification: %s", msg.value.decode("utf-8"))
        lg.debug("\tData Timestamp: %s", msg.timestamp)
        lg.debug("\tCurrent Timestamp: {calendar.timegm(time.gmtime())}")
        lg.debug(
            "Time between modification and consume: %d seconds",
            calendar.timegm(time.gmtime()) - msg.timestamp,
        )
        try:
            post = requests.post(
                f"http://{os.getenv('API_ELASTIC_HOST')}:{os.getenv('API_ELASTIC_PORT')}/modify",
                json=json.loads(msg.value.decode("utf-8")),
                timeout=5,
            )
            lg.debug("Response from API: %s", post.text)
        except requests.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)


def delete(msg: ConsumerRecord) -> None:
    """Function that receives a ConsumerRecord object and logs a deletion message.

    Args:
        msg (ConsumerRecord): ConsumerRecord object containing the message received.

    Returns:
        None.
    """
    lg.info("Delete: %s", msg.value.decode("utf-8"))
    lg.debug(
        "Time between delete and consume: %d seconds",
        calendar.timegm(time.gmtime()) - msg.timestamp,
    )
    try:
        post = requests.post(
            f"http://{os.getenv('API_ELASTIC_HOST')}:{os.getenv('API_ELASTIC_PORT')}/delete",
            json=json.loads(msg.value.decode("utf-8")),
            timeout=5,
        )
        lg.debug("Response from API: %s", post.text)
    except requests.exceptions.ConnectionError as error:
        lg.error("Connection error: %s", error)


def received_from_topic(msg: ConsumerRecord) -> None:
    """Function that receives a ConsumerRecord object and executes a function based on the received subject.

    Args:
        msg (ConsumerRecord): ConsumerRecord object containing the received message.

    Returns:
        None.
    """
    # Create a dictionary with the topic and a function to be executed
    actual_topics = {
        "insert": insert,
        "update": modification,
        "delete": delete,
    }
    try:
        actual_topics[json.loads(msg.value.decode("utf-8"))["type"]](msg)
    except KeyError:
        lg.error(
            "Received message from topic %s but no function is defined for it",
            msg.topic,
        )


def consume(consumer: KafkaConsumer, topic: str) -> None:
    """Function that consumes messages from a Kafka server.

    Args:
        consumer (KafkaConsumer): object KafkaConsumer that consumes messages from Kafka.

    Returns:
        None.
    """
    start_timestamp = calendar.timegm(time.gmtime())
    lg.debug("Starting consume from topic %s", topic)
    while True:
        try:
            consumer.subscribe([topic])
        except AssertionError as err:
            lg.error("Error: %s", err)
            sys.exit(1)

        for msg in consumer:
            if start_timestamp < msg.timestamp:
                received_from_topic(msg)
                # Set the offset to the next message
                consumer.commit()


def check_environment_variables() -> None:
    """Function that checks if the required environment variables are defined.

    Args:
        None.

    Returns:
        None.

    Raises:
        KeyError: If one of the required environment variables is not defined.
    """
    used_variables = [
        "KAFKA_BROKER",
        "KAFKA_BROKER_PORT",
        "TOPIC_INSERT",
        "TOPIC_UPDATE",
        "TOPIC_DELETE",
    ]
    for variable in used_variables:
        if variable not in os.environ:
            raise KeyError(f"Environment variable {variable} not found")


def create_consumer() -> KafkaConsumer:
    """Function that creates and returns a KafkaConsumer object to consume Kafka messages.

    Args:
        None.

    Returns:
        KafkaConsumer: KafkaConsumer object that consumes Kafka messages.
    """
    configuration = {
        "bootstrap_servers": f"{os.getenv('KAFKA_BROKER')}:{os.getenv('KAFKA_BROKER_PORT')}",
        "client_id": "consumer-python",
        "auto_offset_reset": "earliest",
        "group_id": "consumer-python-group",
    }
    # Retry until the broker is available
    while True:
        try:
            return KafkaConsumer(**configuration)
        except UnrecognizedBrokerVersion as err:
            lg.error("Broker not found.\n\tError: %s", err)
            lg.error("Retrying in 5 seconds...")
            time.sleep(5)
        except NoBrokersAvailable as err:
            lg.error("Broker not found.\n\tError: %s", err)
            lg.error("Retrying in 5 seconds...")
            time.sleep(5)


def start_consumer_service() -> None:
    """Function that starts the consumer service.

    Args:
        None.

    Returns:
        None.
    """
    topics = list(
        set(
            [
                os.getenv("TOPIC_INSERT"),
                os.getenv("TOPIC_UPDATE"),
                os.getenv("TOPIC_DELETE"),
            ]
        )
    )
    if len(topics) == 0:
        lg.error("No topics defined")
        sys.exit(1)
    else:
        try:
            threads = []
            for topic in topics:
                lg.debug("Starting consumer for topic %s", topic)
                thread = threading.Thread(target=consume, args=(create_consumer(), topic,))
                threads.append(thread)
                thread.start()

            # Esperar a que todos los hilos terminen antes de salir
            for thread in threads:
                thread.join()
        except Exception as err:
            for thread in threads:
                # Forzar la terminación de los hilos
                thread._stop()
            lg.error("Error: %s", err)

def main() -> None:
    """Main program function that creates a Kafka consumer and consumes messages from the specified topics.

    Args:
        None.

    Returns:
        None.
    """
    try:
        check_environment_variables()
    except KeyError as err:
        lg.error("Error: %s", err)
        sys.exit(1)
    start_consumer_service()


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
    main()
