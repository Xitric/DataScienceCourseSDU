const mysql = require("mysql");

class MySqlClient {

    // TODO: Use environment variables
    constructor() {
        this.host = "localhost";
        this.user = "client";
        this.password = "H8IAQzX236eu5Ep0";
        this.database = "analysis_results";
    }

    getConnection(onError, onConnection) {
        let connection = mysql.createConnection({
            host: this.host,
            user: this.user,
            password: this.password,
            database: this.database
        });

        connection.connect(err => {
            if (err) {
                onError(err);
            } else {
                onConnection(connection);
            }
        });
    }

    getDailyServiceRatesForCategory(category, onResult) {
        const sql = "SELECT neighborhood, rate, day FROM service_cases_daily WHERE category = ?";

        this.getConnection(err => {
            if (err) console.log(err);
        }, connection => {
            connection.query(sql, category, (err, results) => {
                if (err) console.log(err);
                onResult(results);
            });
        });
    }

    getServiceRatesForCategory(category) {
    //    Is this useful?
    }
}

module.exports = MySqlClient;
