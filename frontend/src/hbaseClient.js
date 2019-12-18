const hbase = require('hbase');

class HbaseClient {

    constructor() {
        this.client = hbase({ host: '172.200.0.10', port: 8080 })
        console.log("constructor called hbaseclient")
    }

    testTable() {
        console.log("testTable() called in hbaseclient")

        this.client.table('modern_incident_reports')
            .regions(function(error, regions) {
                console.info('Error: ' + error)
                console.info(regions)
            });

        // this.client.table('modern_incident_reports').schema(function(error, schema) {
        //     console.log("Error: " + error)
        //     console.info(schema)
        // });

    }
}

module.exports = HbaseClient;