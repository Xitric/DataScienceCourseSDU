const request = require('request');

class LivyClient {

    constructor() {
        this.baseUrl = "http://livy:8998";
    }

    batchSubmit(name, parameters, onComplete) {
        let options = {
            url: this.baseUrl + "/batches",
            method: "POST",
            json: {
                jars: [
                    "hdfs://namenode:9000/apps/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar",
                    "hdfs://namenode:9000/apps/mysql-connector-java-8.0.18.jar"
                ],
                pyFiles: [
                    "hdfs://namenode:9000/apps/files.zip",
                ],
                conf: {
                    "spark.jars.packages": "org.apache.spark:spark-streaming-flume_2.11:2.4.4"
                },
                file: "hdfs://namenode:9000/apps/" + name + ".py",
                args: parameters
            }
        };

        request(options, function (err, res, body) {
            onComplete(body);
        });
    }

    batchQuery(id, onComplete) {
        let options = {
            url: this.baseUrl + "/batches/" + id,
            method: "GET"
        };

        request(options, function (err, res, body) {
            if (body == undefined) {
                onComplete(body);
            }
            else {
                onComplete(JSON.parse(body));
            }
        });
    }
}

module.exports = LivyClient;
