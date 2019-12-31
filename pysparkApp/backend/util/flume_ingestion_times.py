import datetime


def update_ingestion_times(data_source: str, latest: datetime):
    import mysql.connector

    db_connection = mysql.connector.connect(host="mysql",
                                            user="spark",
                                            passwd="P18YtrJj8q6ioevT",
                                            database="flume")
    query = "INSERT INTO data_ingestion_latest (data_source, latest) VALUES (%s,%s)" \
            "ON DUPLICATE KEY UPDATE latest = VALUES(latest);"
    insert_tuple = (data_source, latest)
    cursor = db_connection.cursor(prepared=True)
    cursor.execute(query, insert_tuple)
    db_connection.commit()
    cursor.close()
    db_connection.close()
