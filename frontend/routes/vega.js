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
                        incidentCategories: incidentCategories,
                        data: results,
                        category: category
                    });
                });
            } else if (type === "incident") {
                mysqlClient.getMonthlyIncidentRatesForCategory(category, results => {
                    res.render('vega_choro', {
                        title: 'Choropleth Visualization',
                        script: 'vega_vis_choro',
                        layout: 'layout_vega',
                        stylesheets: ["style_graph"],
                        serviceCategories: serviceCategories,
                        incidentCategories: incidentCategories,
                        data: results,
                        category: category
                    });
                });
            }
        });
    });
});

router.get('/cluster', function (req, res) {
    let mysqlClient = new MySqlClient();
    let livyClient = new LivyClient();
    
    let batchId = req.cookies.batchId;
    let state;

    livyClient.batchQuery(batchId, body => {
        state = body.state;

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
                            data: results,
                            state: state
                        });
                    } else {
                        res.render('vega_cluster', {
                            title: 'Cluster Visualization',
                            script: 'vega_vis_cluster',
                            layout: 'layout_vega',
                            stylesheets: ["style_graph"],
                            serviceCategories: serviceCategories,
                            incidentCategories: incidentCategories,
                            data: [],
                            state: state
                        });
                    }
                });
            });
        });
    });
});

router.get('/cluster/submit', function (req, res) {
    let livyClient = new LivyClient();

    let batchId = req.cookies.batchId;
    let state;

    livyClient.batchQuery(batchId, body => {
        state = body.state;

        // Don't allow the user to run multiple jobs at a time
        if (state == "running") {
            res.redirect('/vega/cluster');
        }
        else {
            let serviceCategories = "";
            let incidentCategories = "";
            
            // Handles whether the req has a nothing, just a string, or an array
            if (req.query.services != undefined) {
                if (typeof(req.query.services) == "string") {
                    serviceCategories = req.query.services;
                }
                else {
                    serviceCategories = req.query.services.join(";").split(' ').join('+');
                }
            }
            if (req.query.incidents != undefined) {
                if (typeof(req.query.incidents) == "string") {
                    incidentCategories = req.query.incidents;
                }
                else {
                    incidentCategories = req.query.incidents.join(";").split(' ').join('+');
                }
            }

            // Start a batch submit for a k-means analysis
            livyClient.batchSubmit('k-means', [serviceCategories, incidentCategories], body => {

                // Save the current batch id in the cookie
                res.cookie('batchId', body.id, {
                    maxAge: 60*60*24,
                    httpOnly: true
                });

                res.redirect('/vega/cluster');
            });
        }
        
    });
});

router.get('/horizon', function (req, res) {
    let mysqlClient = new MySqlClient();
    let type = req.query.type || "service";
    let category = req.query.category || "graffiti";

    mysqlClient.getAvailableServiceCategories(serviceCategories => {
        mysqlClient.getAvailableIncidentCategories(incidentCategories => {
            mysqlClient.getNeighborhoodNames(names => {
                if (type === "service") {
                    mysqlClient.getDailyServiceRatesForCategory(category, horizonData => {
                        res.render('vega_horizon', {
                            title: 'Horizon Visualization',
                            script: 'vega_vis_horizon',
                            layout: 'layout_vega',
                            serviceCategories: serviceCategories,
                            incidentCategories: incidentCategories,
                            data: names,
                            horizonData: [horizonData, type, category],
                            category: category,
                            type: type
                        });
                    });
                } else if (type === "incident") {
                    mysqlClient.getDailyIncidentRatesForCategory(category, horizonData => {
                        res.render('vega_horizon', {
                            title: 'Horizon Visualization',
                            script: 'vega_vis_horizon',
                            layout: 'layout_vega',
                            serviceCategories: serviceCategories,
                            incidentCategories: incidentCategories,
                            data: names,
                            horizonData: [horizonData, type, category],
                            category: category,
                            type: type
                        });
                    });
                }
            });
        });
    });
});

router.get('/scatter', function (req, res) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getMonthlyServiceRates(services => {
        mysqlClient.getMonthlyIncidentRates(incidents => {
            let data = [services, incidents];
            res.render('vega_scatter', {
                title: 'incident and Service Correlations',
                script: 'vega_vis_scatter',
                layout: 'layout_vega',
                stylesheets: ["style_graph"],
                data: data
            });
        });
    });
});

module.exports = router;
