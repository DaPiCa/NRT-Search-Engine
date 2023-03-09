import time
import logging as lg
import sys
import mysql.connector


def db_watcher():
    retry = True
    cont = 0
    conn = None
    while retry:
        try:
            conn = mysql.connector.connect(
                host="host.docker.internal",
                port="3306",
                user="root",
                password="user",
                database="classicmodels",
            )
            if conn.is_connected():
                retry = False
                db_info = conn.get_server_info()
                lg.info("Connected to MySQL Server version: %s", db_info)
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                lg.debug("You're connected to database: %s", record)
                cursor.execute("select * from offices")
                entrys = cursor.fetchall()
                for entry in entrys:
                    lg.info("%s", entry)

        except mysql.connector.Error as err:
            lg.error("Connection failed, retrying in 5 seconds.\n\tError: %s", err)
            time.sleep(5)
            cont += 1
            if cont == 5:
                retry = False
                lg.error("Connection failed, exiting")
                sys.exit(1)
        finally:
            if conn is not None and conn.is_connected():
                cursor.close()
                conn.close()
                lg.info("MySQL connection is closed")


if __name__ == "__main__":
    lg.basicConfig(
        format="%(asctime)s | %(filename)s | %(levelname)s |>> %(message)s",
        level=lg.DEBUG,
    )
    db_watcher()
