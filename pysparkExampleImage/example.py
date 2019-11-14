from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import os

NAMENODE_ADDRESS = "172.200.0.2"
NAMENODE_IPC_PORT = 9000
DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

if __name__ == "__main__":
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://nexus-private.hortonworks.com/nexus/content/groups/public/,http://repo.hortonworks.com/content/repositories/releases/,https://repo.hortonworks.com/content/groups/public --files /Users/Kasper/Desktop/hbase-site.xml pyspark-shell'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 pyspark-shell'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.3-2.4-s_2.11 pyspark-shell'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc:1.1.0.2.6.2.0-205 --repositories http://repo.hortonworks.com/content/repositories/releases/ pyspark-shell'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--files /Users/Kasper/Desktop/hbase-site.xml pyspark'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars  lib/shc-core-1.1.3-2.4-s_2.11.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /Users/Kasper/Desktop/hbase-site.xml pyspark'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = ("--repositories http://repo.hortonworks.com/content/groups/public/ " "--packages com.hortonworks:shc-core:1.1.1-1.6-s_2.10 " " pyspark-shell")
    # os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master yarn " "--repositories https://repo.hortonworks.com/content/repositories/releases/ " "--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 " " --files /app/hbase-site.xml " " pyspark-shell")
    os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master yarn " "--repositories https://repo.hortonworks.com/content/repositories/releases/ " "--packages com.hortonworks.shc:shc-core:1.1.0.3.1.2.1-1 " " --files /app/hbase-site.xml " " pyspark-shell")

    # Run Spark locally
    spark_configuration = SparkConf()\
        .set("spark.driver.host", "127.0.0.1")\
        .set("spark.hbase.host", "hbase:16010")
    sc = SparkContext(master="local", appName="CrimeMapper", conf=spark_configuration)

    sql = SQLContext(sc)

    # Some weird stuff I found
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"ServiceCases"},
        "rowkey":"key",
        "columns":{
            "col0":{"cf":"rowkey", "col":"key", "type":"string"},
            "col1":{"cf":"FamilyA", "col":"Name", "type":"string"},
            "col2":{"cf":"FamilyA", "col":"Age", "type":"int"}
        }
    }""".split())

    df = sql.read.options(catalog=catalog).format(DATA_SOURCE_FORMAT).load()
    print(df.count())
    
