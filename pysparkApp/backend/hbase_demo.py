from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from person import Person
import os

DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--master yarn --jars /backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar pyspark-shell"

    # Run Spark locally
    spark_configuration = SparkConf().set("spark.driver.host", "localhost")
    sc = SparkContext(master="local", appName="CrimeMapper", conf=spark_configuration)
    sql = SQLContext(sc)

    # Create some data
    df = sc.parallelize({Person("4", "Kasper", "24", "Sailing"),
                         Person("5", "John", "15", "Drawing"),
                         Person("6", "Catherine", "28", "Tennis")}).toDF()

    # Describes how to interpret the contents of the HBase table
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"Person"},
        "rowkey":"key",
        "columns":{
            "id":{"cf":"rowkey", "col":"key", "type":"string"},
            "name":{"cf":"Info", "col":"Name", "type":"string"},
            "age":{"cf":"Info", "col":"Age", "type":"string"},
            "hobby":{"cf":"Hobby", "col":"Primary", "type":"string"}
        }
    }""".split())

    # Store data in table
    df.write.options(catalog=catalog, newtable="5").format(DATA_SOURCE_FORMAT).save()

    # Read the table contents
    df = sql.read.options(catalog=catalog).format(DATA_SOURCE_FORMAT).load()
    df.show()
