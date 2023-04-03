import elasticsearch
import mysql.connector
import os
import logging as lg
import logging.config as lg_conf
import sys
import time
import json

def connect_to_database():
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

def connect_to_elastic():
    ELASTIC_SEARCH_SERVER = f"http://{os.getenv('ELASTIC_HOST')}:{os.getenv('ELASTIC_PORT')}"
    es = elasticsearch.Elasticsearch(ELASTIC_SEARCH_SERVER,
                       use_ssl = False,
                       ca_certs=False,
                       verify_certs=False)
    while True:
        try:
            es.info()
            lg.info("Connected to ElasticSearch")
            return es
        except elasticsearch.exceptions.ConnectionError as e:
            lg.error("Connection error: %s", e)
            lg.error("Retrying in 5 seconds...")
            time.sleep(5)
               

def remove(conn, elastic):
    # Take all tables from the database and remove them from ElasticSearch
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        lg.info("Removing database from ElasticSearch. Please wait...")
        for table in tables:
            table_name = table[0]
            elastic.indices.delete(index=table_name, ignore=[400, 404])
        lg.info("Database removed from ElasticSearch")
    
def insert(conn, elastic):
    # Take all tables from the database and insert them into ElasticSearch
    remove(conn, elastic)
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        lg.info("Inserting database into ElasticSearch. Please wait...")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            for row in rows:
                row_dict = {}
                for i in range(len(row)):
                    row_dict[cursor.column_names[i]] = row[i]
                elastic.index(index=table, document=row_dict)
        
        
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