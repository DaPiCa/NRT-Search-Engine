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
    Creates a connection to the MySQL database using the connection parameters provided by the environment variables
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_ROOT_USER (if root=True), MYSQL_PASSWORD (if root=False), MYSQL_ROOT_PASSWORD (if root=True) and MYSQL_DATABASE.

    Args:
    - root (bool, optional): if True, the root user and password defined in the environment variables are used.

    Returns:
    - mysql.connector.connection.MySQLConnection or None: Returns a connection to the database if the connection could be established, otherwise returns None.

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
    except TypeError as err:
        lg.error("Environment variables not set, exiting.\n\tError: %s", err)    
        sys.exit(1)
    return connection


def insert_event(event: WriteRowsEvent) -> None:
    """
    Function in charge of handling data insertion events.

    Args:
        event (WriteRowsEvent): event that represents a data insertion in the database.

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
    Function that is responsible for handling data update events.

    Args:
        event (UpdateRowsEvent): event that represents a data update in the database.

    Returns:
        None
    """
    lg.info("Received Update Event")
    for row in event.rows:
        lg.info("\tUpdated row:")
        for key in row["after_values"]:
            lg.info(
                "\t\tValue %s: %s => %s",
                key,
                row["before_values"][key],
                row["after_values"][key],
            )


def delete_event(event: DeleteRowsEvent) -> None:
    """
    Function that is responsible for handling data deletion events.

    Args:
        event (DeleteRowsEvent): event that represents a data deletion in the database.

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
    Checks if the current user has the necessary permissions to perform a replication.

    Args:
        connection: object of type MySQLConnection set to the database.

    Returns:
        permission (bool): true if the current user has the necessary permissions, False otherwise.
    """
    permission = False
    with connection.cursor() as cursor:
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        results = cursor.fetchall()
        for row in results:
            if "REPLICATION SLAVE" in row[0]:
                permission = True
                break
    return permission


def permission_grant() -> None:
    """
    Grants replication permissions to the current user.
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
        cursor.execute(f"GRANT REPLICATION SLAVE ON *.* TO '{user}'@'%';")
        temporal_connection.commit()
        temporal_connection.close()


def listen_for_changes() -> None:
    """
    Listens and processes events in the MySQL binary log.
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
    Attempts to establish a connection with a database and listen for changes. If the connection fails or 
    permission to access the database is denied, the function will retry a limited number of times before 
    ultimately failing and exiting.
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
