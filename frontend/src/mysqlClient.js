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

    perform(sql, values, onResult) {
        this.getConnection(err => {
            if (err) console.log(err);
        }, connection => {
            connection.query(sql, values, (err, results) => {
                if (err) console.log(err);
                onResult(results);
            });
        });
    }

    getDailyServiceRatesForCategory(category, onResult) {
        const sql = "SELECT neighborhood, rate, day FROM service_cases_daily WHERE category = ?";
        this.perform(sql, [category], onResult);
    }

    getMonthlyServiceRates(onResult) {
        const sql = "SELECT * FROM service_cases_monthly";
        this.perform(sql, [], onResult);
    }

    getMonthlyServiceRatesForCategory(category, onResult) {
        const sql = "SELECT neighborhood, `" + category + "` as rate FROM service_cases_monthly";
        this.perform(sql, [], onResult);
    }

    getAvailableServiceCategories(onResult) {
        const sql = "DESC service_cases_monthly";
        this.perform(sql, [], results => {
            results.splice(0, 2);
            onResult(results.map(v => v.Field));
        });
    }

    getAvailableIncidentCategories(onResult) {
        const sql = "DESC incident_cases_monthly";
        this.perform(sql, [], results => {
            results.splice(0, 2);
            onResult(results.map(v => v.Field));
        });
    }
}

module.exports = MySqlClient;
