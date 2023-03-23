# pylint: disable=import-error, line-too-long

import logging as lg
import logging.config as lg_conf
import sys
import json
import os
import calendar
import time
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import UnrecognizedBrokerVersion, NoBrokersAvailable


def insert(msg: ConsumerRecord) -> None:
    """Función que recibe un objeto ConsumerRecord y registra un mensaje de inserción.

    Args:
        msg (ConsumerRecord): Objeto ConsumerRecord que contiene el mensaje recibido.

    Returns:
        None.
    """
    lg.info("Insert: %s", msg.value.decode('utf-8'))
    lg.debug(
        "Time between insert and consume: %d seconds", calendar.timegm(time.gmtime())-msg.timestamp)


def modification(msg: ConsumerRecord) -> None:
    """Función que recibe un objeto ConsumerRecord y registra un mensaje de modificación.

    Args:
        msg (ConsumerRecord): Objeto ConsumerRecord que contiene el mensaje recibido.

    Returns:
        None.
    """
    lg.info("Modification: %s", msg.value.decode('utf-8'))
    lg.debug("\tData Timestamp: %s", msg.timestamp)
    lg.debug("\tCurrent Timestamp: {calendar.timegm(time.gmtime())}")
    lg.debug(
        "Time between modification and consume: %d seconds", calendar.timegm(time.gmtime())-msg.timestamp)


def delete(msg: ConsumerRecord) -> None:
    """Función que recibe un objeto ConsumerRecord y registra un mensaje de eliminación.

    Args:
        msg (ConsumerRecord): Objeto ConsumerRecord que contiene el mensaje recibido.

    Returns:
        None.
    """
    lg.info("Delete: %s", msg.value.decode('utf-8'))
    lg.debug(
        "Time between delete and consume: %d seconds", calendar.timegm(time.gmtime())-msg.timestamp)


def received_from_topic(msg: ConsumerRecord) -> None:
    """Función que recibe un objeto ConsumerRecord y ejecuta una función en función del tema recibido.

    Args:
        msg (ConsumerRecord): Objeto ConsumerRecord que contiene el mensaje recibido.

    Returns:
        None.
    """
    # Create a dictionary with the topic and a function to be executed
    actual_topics = {
        os.getenv("TOPIC_INSERT"): modification,
        os.getenv("TOPIC_UPDATE"): modification,
        os.getenv("TOPIC_DELETE"): delete, }
    try:
        actual_topics[msg.topic](msg)
    except KeyError:
        lg.error(
            "Received message from topic %s but no function is defined for it", msg.topic)


def consume(consumer: KafkaConsumer) -> None:
    """Función que consume mensajes de un servidor de Kafka.

    Args:
        consumer (KafkaConsumer): Objeto KafkaConsumer que consume mensajes de Kafka.

    Returns:
        None.
    """
    start_timestamp = calendar.timegm(time.gmtime())
    while True:
        try:
            consumer.subscribe(list(set([os.getenv("TOPIC_INSERT"), os.getenv(
                "TOPIC_UPDATE"), os.getenv("TOPIC_DELETE")])))
        except AssertionError as err:
            lg.error("Error: %s", err)
            sys.exit(1)

        for msg in consumer:
            if start_timestamp < msg.timestamp:
                received_from_topic(msg)
                # Set the offset to the next message
                consumer.commit()


def check_environment_variables() -> None:
    """Función que comprueba si las variables de entorno necesarias están definidas.

    Args:
        None.

    Returns:
        None.

    Raises:
        Exception: Si una de las variables de entorno necesarias no está definida.
    """
    used_variables = ["KAFKA_BROKER", "KAFKA_BROKER_PORT",
                      "TOPIC_INSERT", "TOPIC_UPDATE", "TOPIC_DELETE"]
    for variable in used_variables:
        if variable not in os.environ:
            raise KeyError(f"Environment variable {variable} not found")


def create_consumer() -> KafkaConsumer:
    """Función que crea y devuelve un objeto KafkaConsumer para consumir mensajes de Kafka.

    Args:
        None.

    Returns:
        KafkaConsumer: Objeto KafkaConsumer que consume mensajes de Kafka.
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


def main() -> None:
    """Función principal del programa que crea un consumidor de Kafka y consume mensajes de los topics especificados.

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
    consumer = create_consumer()
    consume(consumer)


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
