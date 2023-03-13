# pylint: disable=import-error
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


def connection_manager(root=False):
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


def insert_event(event):
    lg.info("Received Insert Event")
    for row in event.rows:
        lg.info("\tInserted row:")
        for key in row["values"]:
            lg.info("\t\t%s : %s", key, row["values"][key])


def update_event(event):
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


def delete_event(event):
    lg.info("Received Delete Event")
    for row in event.rows:
        lg.info("\tDeleted row:")
        for key in row["values"]:
            lg.info("\t\t%s : %s", key, row["values"][key])


def permissions_check(connection):
    permission = False
    with connection.cursor() as cursor:
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        results = cursor.fetchall()
        for row in results:
            if "REPLICATION CLIENT" in row[0]:
                permission = True
                break
    return permission


def permission_grant():
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
        cursor.execute(f"GRANT REPLICATION CLIENT ON *.* TO '{user}'@'%';")
        temporal_connection.commit()
    temporal_connection.close()


def listen_for_changes():
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
            if event.timestamp > start_timestamp and event.rows is not None:  # type: ignore
                if isinstance(event, WriteRowsEvent):
                    insert_event(event)

                if isinstance(event, UpdateRowsEvent):
                    update_event(event)

                if isinstance(event, DeleteRowsEvent):
                    delete_event(event)
    finally:
        if stream is not None:
            stream.close()


def main():
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
