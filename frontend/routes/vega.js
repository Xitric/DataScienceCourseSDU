let MySqlClient = require('../src/mysqlClient');

let express = require('express');
let router = express.Router();

/* GET home page. */
router.get('/', function (req, res) {
    res.render('vega', {title: 'Vega Visualization', script: 'vega_vis', layout: 'layout_vega'});
});

router.get('/choro', function (req, res) {
    let mysqlClient = new MySqlClient();
    let type = req.query.type || "service";
    let category = req.query.category || "graffiti";

    mysqlClient.getAvailableServiceCategories(serviceCategories => {
        mysqlClient.getAvailableIncidentCategories(incidentCategories => {
            if (type === "service") {
                mysqlClient.getMonthlyServiceRatesForCategory(category, results => {
                    res.render('vega_choro', {
                        title: 'Choropleth Visualization',
                        script: 'vega_vis_choro',
                        layout: 'layout_vega',
                        stylesheets: ["style_graph"],
                        serviceCategories: serviceCategories,
                        crimeCategories: incidentCategories,
                        data: results
                    });
                });
            } else if (type === "crime") {
                mysqlClient.getMonthlyIncidentRatesForCategory(category, results => {
                    res.render('vega_choro', {
                        title: 'Choropleth Visualization',
                        script: 'vega_vis_choro',
                        layout: 'layout_vega',
                        stylesheets: ["style_graph"],
                        serviceCategories: serviceCategories,
                        crimeCategories: incidentCategories,
                        data: results
                    });
                });
            }
        });
    });
});

router.get('/cluster', function (req, res) {
    let mysqlClient = new MySqlClient();
    let data = [{"neighborhood": "Castro", cluster: 0},
        {"neighborhood": "Diamond Heights", cluster: 0},
        {"neighborhood": "Bayview", cluster: 1},
        {"neighborhood": "Russian Hill", cluster: 0},
        {"neighborhood": "Civic Center", cluster: 2},
        {"neighborhood": "Nob Hill", cluster: 3},
        {"neighborhood": "Excelsior", cluster: 3},
        {"neighborhood": "Pacific Heights", cluster: 0}
    ];

    mysqlClient.getAvailableServiceCategories(serviceCategories => {
        mysqlClient.getAvailableIncidentCategories(incidentCategories => {
            res.render('vega_cluster', {
                title: 'Cluster Visualization',
                script: 'vega_vis_cluster',
                layout: 'layout_vega',
                serviceCategories: serviceCategories,
                incidentCategories: incidentCategories,
                data: data
            });
        });
    });
});

router.get('/horizon', function (req, res) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getDailyServiceRatesForCategory("Encampments", results => {
        //TODO: Remove neighborhoods.txt
        res.render('vega_horizon', {
            title: 'Horizon Visualization',
            script: 'vega_vis_horizon',
            layout: 'layout_vega',
            data: 'public/dataset/neighborhoods.txt',
            horizonData: results
        });
    });
});

router.get('/scatter', function (req, res) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getMonthlyServiceRates(services => {
        mysqlClient.getMonthlyIncidentRates(incidents => {
            let data = [services, incidents];
            res.render('vega_scatter', {
                title: 'Crime and Service Correlations',
                script: 'vega_vis_scatter',
                layout: 'layout_vega',
                stylesheets: ["style_graph"],
                data: data
            });
        });
    });
});

module.exports = router;
