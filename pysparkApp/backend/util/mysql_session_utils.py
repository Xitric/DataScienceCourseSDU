import mysql
from mysql.connector import (connection)

def get_mysql_session_instance():
    if "mysqlSessionSingletonInstance" not in globals() or \
        not globals()["mysqlSessionSingletonInstance"].is_connected():
        db_connection = mysql.connector.connect(host="mysql",
                                                user="spark",
                                                passwd="P18YtrJj8q6ioevT",
                                                database="analysis_results")
        globals()["mysqlSessionSingletonInstance"] = db_connection
    return globals()["mysqlSessionSingletonInstance"]
