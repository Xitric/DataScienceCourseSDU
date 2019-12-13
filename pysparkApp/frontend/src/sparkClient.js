const request = require('request');

class SparkClient {

    constructor() {
        this.baseUrl = "http://localhost:8080";
    }

    ingestDataStreaming(dataset) {
        request.post(this.baseUrl + "/ingest/streaming", {
            json: {
                dataset: dataset
            }
        }, (err, res, body) => {
            if (! err) {
                console.log("Success!")
            } else {
                console.log("You are a failure!")
            }
        });
    }
}

module.exports = SparkClient;