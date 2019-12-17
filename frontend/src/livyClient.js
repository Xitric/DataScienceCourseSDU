const request = require('request');

class LivyClient {

    constructor() {
        this.baseUrl = "http://livy:8998";
    }

    batchSubmit(name, version, onComplete) {
        let options = {
            url: this.baseUrl + "/batches",
            method: "POST",
            json: {
                jars: [
                    "hdfs://namenode:9000/apps/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar"
                ],
                pyFiles: [
                    "hdfs://namenode:9000/apps/files.zip",
                    // "hdfs://namenode:9000/apps/context.py",
                    // "hdfs://namenode:9000/apps/incident_modern_context.py",
                    // "hdfs://namenode:9000/apps/service_case_context.py",
                    // "hdfs://namenode:9000/apps/string_hasher.py",
                    // "hdfs://namenode:9000/apps/service_aggregation_context.py"
                ],
                conf: {
                    "spark.jars.packages": "org.apache.spark:spark-streaming-flume_2.11:2.4.4"
                },
                file: "hdfs://namenode:9000/apps/" + name + ".py"
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
            onComplete(body);
        });
    }

















    createSession(onComplete) {
        let options = {
            url: this.baseUrl + "/sessions",
            method: "POST",
            json: {
                kind: "pyspark"
            }
        };

        let client = this;
        request(options, function (err, res) {
            let location = res.headers.location;
            client.waitForBoot(location, onComplete);
        });
    }

    waitForBoot(location, onComplete) {
        let options = {
            url: this.baseUrl + location,
            method: "GET"
        };

        let client = this;
        request(options, function (err, res, body) {
            if (JSON.parse(body).state === "idle") {
                onComplete(JSON.parse(body));
            } else {
                setTimeout(client.waitForBoot.bind(client, location, onComplete), 1000);
            }
        });
    }

    execute(session, job, onComplete) {
        let options = {
            url: this.baseUrl + "/sessions/" + session.id + "/statements",
            method: "POST",
            json: {
                code: job
            }
        };

        let client = this;
        request(options, function (err, res, body) {
            client.updateSession(session, body);
            console.log(res.headers);
            onComplete(session);
        });
    }

    queryResult(session, onComplete) {
        let options = {
            url: this.baseUrl + "/sessions/" + session.id + "/statements/0",
            method: "GET"
        };

        let client = this;
        request(options, function (err, res, body) {
            client.updateSession(session, JSON.parse(body));
            console.log(body);
            onComplete(session);
        });
    }

    kill(session, onComplete) {
        let options = {
            url: this.baseUrl + "/sessions/" + session.id,
            method: "DELETE"
        };

        request(options, function () {
            session.state = "stopped";
            onComplete(session);
        });
    }
    
    updateSession(session, newData) {
        session.id = newData.id || session.id;
        session.name = newData.name || session.id;
        session.appId = newData.appId || session.appId;
        session.owner = newData.owner || session.owner;
        session.proxyUser = newData.proxyUser || session.proxyUser;
        session.state = newData.state || session.state;
        session.kind = newData.kind || session.kind;
        console.log(newData);
        session.output = newData.output || session.output;
    }
}

module.exports = LivyClient;
