import os
import sys
import time
import logging as lg
import logging.config as lg_conf

import mysql.connector  # pylint: disable=import-error
import elasticsearch  # pylint: disable=import-error


def connect_to_database() -> mysql.connector.connection.MySQLConnection:
    """
    Creates a connection to the MySQL database using the connection parameters
    provided by the environment variables
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD and MYSQL_DATABASE.

    Args:
    - None

    Returns:
    - mysql.connector.connection.MySQLConnection: Returns a connection to the database if
    the connection could be established, otherwise returns None.

    """
    connection = None
    while True:
        try:
            connection = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST"),
                port=os.getenv("MYSQL_PORT"),
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE"),
            )
            lg.info("Connected to database")
            break
        except mysql.connector.Error as err:
            lg.error("Connection failed, retrying in 5 seconds.\n\tError: %s", err)
            time.sleep(5)
        except TypeError as err:
            lg.error("Environment variables not set, exiting.\n\tError: %s", err)
            sys.exit(1)
    return connection


def connect_to_elastic() -> elasticsearch.Elasticsearch:
    """
    Connects to the Elasticsearch server specified by the environment variables
    ELASTIC_HOST and ELASTIC_PORT.
    Retries indefinitely until a connection is established.

    Returns:
    An instance of the Elasticsearch client connected to the Elasticsearch server.
    """
    elastic_search_server_parameters = (
        f"http://{os.getenv('ELASTIC_HOST')}:{os.getenv('ELASTIC_PORT')}"
    )
    es_client = elasticsearch.Elasticsearch(
        elastic_search_server_parameters,
        use_ssl=False,
        ca_certs=False,
        verify_certs=False,
    )
    while True:
        try:
            es_client.info()
            lg.info("Connected to ElasticSearch")
            return es_client
        except elasticsearch.exceptions.ConnectionError as error:
            lg.error("Connection error: %s", error)
            lg.error("Retrying in 5 seconds...")
            time.sleep(5)


def remove(
    connector: mysql.connector.connection.MySQLConnection,
    elastic_connection: elasticsearch.Elasticsearch,
) -> None:
    """
        The remove function takes a MySQL database connection and an Elasticsearch client as
        input and removes all the tables from the database from Elasticsearch.

    The function first retrieves all the tables from the database using a SHOW TABLES command and
    iterates over them. For each table, it checks if an index exists in Elasticsearch with the same
    name as the table. If an index exists, it removes all the indexed documents and then deletes
    the index.

    After all the tables have been processed, the function logs a message indicating that the
    database has been removed from Elasticsearch.

    Args:
    connector (mysql.connector.connection.MySQLConnection): A MySQL database connection object.
    elastic_connection (elasticsearch.Elasticsearch): An Elasticsearch client object.

    Returns:
    None. The function performs the necessary actions on Elasticsearch to remove the database.
    """
    with connector.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        lg.info("Removing database from ElasticSearch. Please wait...")
        for table in tables:
            # First check if the index exists in ElasticSearch
            if elastic_connection.indices.exists(index=table[0]):
                lg.info("\tRemoving indexed documents from %s", table[0])
                table_name = table[0]
                query = {"query": {"match_all": {}}}
                elastic_connection.delete_by_query(index=table_name, body=query)
                elastic_connection.indices.delete(index=table_name)
        lg.info("Database removed from ElasticSearch")


def insert(
    connector: mysql.connector.connection.MySQLConnection,
    elastic_connection: elasticsearch.Elasticsearch,
):
    """
    The insert function takes a MySQL database connection and an Elasticsearch
    connection as parameters.
    It inserts all tables from the MySQL database into Elasticsearch.
    The function first removes any existing data in Elasticsearch using the remove function.
    Then, it retrieves all table names from the database using the SHOW TABLES query and iterates
    through each table to retrieve all rows.
    Each row is transformed into a dictionary, where the keys correspond to the column names
    and the values correspond to the row values.
    Finally, each row dictionary
    is indexed into Elasticsearch using the index function. After all tables have been indexed,
    the database and cursor connections are closed.

    Args:
    connector (mysql.connector.connection.MySQLConnection): A MySQL database connection.
    elastic_connection (elasticsearch.Elasticsearch): An Elasticsearch connection.

    Returns:
    None.

    """
    # Take all tables from the database and insert them into ElasticSearch
    remove(connector, elastic)
    with connector.cursor() as cursor:
        sql = "SHOW %s" % "TABLES"
        cursor.execute(sql)
        tables = cursor.fetchall()
        lg.info("Inserting database into ElasticSearch. Please wait...")
        for table in tables:
            lg.info("\tInserting/Indexing documents from %s", table[0])
            table_name = table[0]
            sql = "SELECT * FROM {}".format(table_name)
            cursor.execute(sql)
            while row := cursor.fetchone():
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[cursor.column_names[i]] = value
                elastic_connection.index(index=table_name, document=row_dict)

    cursor.close()
    conn.close()
    lg.info("Database inserted into ElasticSearch")


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
    conn = connect_to_database()
    elastic = connect_to_elastic()

    insert(conn, elastic)
