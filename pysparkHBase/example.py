from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import os

DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--master yarn --jars /app/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar pyspark-shell"

    # Run Spark locally
    spark_configuration = SparkConf().set("spark.driver.host", "localhost")
    sc = SparkContext(master="local", appName="CrimeMapper", conf=spark_configuration)

    sql = SQLContext(sc)

    # Describes how to interpret the contents of the HBase table
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"ServiceCases"},
        "rowkey":"key",
        "columns":{
            "id":{"cf":"rowkey", "col":"key", "type":"string"},
            "name":{"cf":"FamilyA", "col":"Name", "type":"string"},
            "age":{"cf":"FamilyA", "col":"Age", "type":"string"}
        }
    }""".split())

    # Read the table contents
    df = sql.read.options(catalog=catalog).format(DATA_SOURCE_FORMAT).load()
    for row in df.collect():
        print(row)
