# pylint: disable=import-error, line-too-long

import time
import logging as lg
import logging.config as lg_conf
import sys
import os
import calendar
import mysql.connector
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

RETRY_LIMIT = 5


def connection_manager(
    root: bool = False,
) -> mysql.connector.connection.MySQLConnection or None:
    """
    Crea una conexión a la base de datos MySQL utilizando los datos de conexión proporcionados por las variables de entorno
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_ROOT_USER (si root=True), MYSQL_PASSWORD (si root=False), MYSQL_ROOT_PASSWORD (si root=True) y MYSQL_DATABASE.

    Args:
    - root (bool, opcional): Si es True, se utiliza el usuario y contraseña de root definidos en las variables de entorno.

    Returns:
    - mysql.connector.connection.MySQLConnection or None: Devuelve una conexión a la base de datos si se ha podido establecer la conexión, en caso contrario devuelve None.

    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER") if not root else os.getenv("MYSQL_ROOT_USER"),
            password=os.getenv("MYSQL_PASSWORD")
            if not root
            else os.getenv("MYSQL_ROOT_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
        )
    except mysql.connector.Error as err:
        lg.error("Connection failed, retrying in 5 seconds.\n\tError: %s", err)
        time.sleep(5)
    return connection


def insert_event(event: WriteRowsEvent) -> None:
    """
    Función que se encarga de manejar los eventos de inserción de datos.

    Args:
        event (WriteRowsEvent): Evento que representa una inserción de datos en la base de datos.

    Returns:
        None
    """
    lg.info("Received Insert Event")
    for row in event.rows:
        lg.info("\tInserted row:")
        for key in row["values"]:
            lg.info("\t\t%s : %s", key, row["values"][key])


def update_event(event: UpdateRowsEvent) -> None:
    """
    Función que se encarga de manejar los eventos de actualización de datos.

    Args:
        event (UpdateRowsEvent): Evento que representa una actualización de datos en la base de datos.

    Returns:
        None
    """
    lg.info("Received Update Event")
    for row in event.rows:
        lg.info("\tUpdated row:")
        for key in row["after_values"]:
            lg.info(
                "Value %s: %s => %s",
                key,
                row["before_values"][key],
                row["after_values"][key],
            )


def delete_event(event: DeleteRowsEvent) -> None:
    """
    Función que se encarga de manejar los eventos de eliminación de datos.

    Args:
        event (DeleteRowsEvent): Evento que representa una eliminación de datos en la base de datos.

    Returns:
        None
    """
    lg.info("Received Delete Event")
    for row in event.rows:
        lg.info("\tDeleted row:")
        for key in row["values"]:
            lg.info("\t\t%s : %s", key, row["values"][key])


def permissions_check(connection: mysql.connector.connection.MySQLConnection) -> bool:
    """
    Comprueba si el usuario actual tiene los permisos necesarios para realizar una replicación.

    Args:
        connection: Objeto de tipo MySQLConnection establecido a la base de datos.

    Returns:
        permission (bool): True si el usuario actual tiene los permisos necesarios, False en caso contrario.
    """
    permission = False
    with connection.cursor() as cursor:
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        results = cursor.fetchall()
        for row in results:
            if "REPLICATION CLIENT" in row[0]:
                permission = True
                break
    return permission


def permission_grant() -> None:
    """
    Otorga permisos de replicación al usuario actual.
    """
    lg.info(
        "User %s does not have permission to replicate, granting as root user...",
        os.getenv("MYSQL_USER"),
    )
    temporal_connection = connection_manager(root=True)
    if temporal_connection is None:
        lg.error("Root connection failed, exiting")
        sys.exit(1)
    with temporal_connection.cursor() as cursor:
        user = os.getenv("MYSQL_USER")
        cursor.execute(f"GRANT REPLICATION CLIENT ON . TO '{user}'@'%';")
        temporal_connection.commit()
        temporal_connection.close()


def listen_for_changes() -> None:
    """
    Escucha y procesa los eventos en el log binario de MySQL.
    """
    lg.info("Listening for changes...")

    mysql_settings = {
        "host": os.getenv("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT")),  # type: ignore
        "user": os.getenv("MYSQL_USER"),
        "passwd": os.getenv("MYSQL_PASSWORD"),
    }

    stream = None

    try:
        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=100,
            blocking=True,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
        )

        start_timestamp = calendar.timegm(time.gmtime())

        for event in stream:
            if event.timestamp > start_timestamp and event.rows is not None:
                if isinstance(event, WriteRowsEvent):
                    insert_event(event)

                if isinstance(event, UpdateRowsEvent):
                    update_event(event)

                if isinstance(event, DeleteRowsEvent):
                    delete_event(event)
    finally:
        if stream is not None:
            stream.close()


def main() -> None:
    """
    Función principal que establece la conexión con la base de datos y comienza a escuchar por eventos de replicación.
    """
    contador_reintentos = 0
    connection = None
    while contador_reintentos < RETRY_LIMIT:
        connection = connection_manager()
        if connection is not None and connection.is_connected():
            try:
                lg.info("Connection established with database %s", connection.database)
                while not permissions_check(connection):
                    permission_grant()
                connection.close()
                listen_for_changes()
                break
            finally:
                connection.close()
                contador_reintentos += 1
    if connection is None or not connection.is_connected():
        lg.error("Connection failed, exiting")
        sys.exit(1)


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
