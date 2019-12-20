let MySqlClient = require('../src/mysqlClient');
let LivyClient = require('../src/livyClient');

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

    mysqlClient.getAvailableServiceCategories(serviceCategories => {
        mysqlClient.getAvailableIncidentCategories(incidentCategories => {
            mysqlClient.getNeighborhoodClusters(results => {
                if (results) {
                    res.render('vega_cluster', {
                        title: 'Cluster Visualization',
                        script: 'vega_vis_cluster',
                        layout: 'layout_vega',
                        stylesheets: ["style_graph"],
                        serviceCategories: serviceCategories,
                        incidentCategories: incidentCategories,
                        data: data
                    });
                } else {
                    res.render('vega_cluster', {
                        title: 'Cluster Visualization',
                        script: 'vega_vis_cluster',
                        layout: 'layout_vega',
                        stylesheets: ["style_graph"],
                        serviceCategories: serviceCategories,
                        incidentCategories: incidentCategories,
                        data: []
                    });
                }
            });
        });
    });
});

router.get('/cluster/submit', function (req, res) {
    let livyClient = new LivyClient();

    let serviceCategories = req.query.services.join(";").split(' ').join('+');
    let incidentCategories = req.query.incidents.join(";").split(' ').join('+');

    livyClient.batchSubmit('k-means', [serviceCategories, incidentCategories], body => {
        //TODO: Save session id?
        console.log(body);
        res.redirect('/vega/cluster');
    });
});

router.get('/horizon', function (req, res) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getDailyServiceRatesForCategory("Encampments", results => {
        //TODO: Remove neighborhoods.txt and replace with database call
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
