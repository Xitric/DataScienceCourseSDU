let vegaLite = require('vega-lite');
let MySqlClient = require('../src/mysqlClient');

let express = require('express');
let router = express.Router();

/* GET home page. */
router.get('/', function (req, res, next) {
    res.render('vega', {title: 'Vega Visualization', script: 'vega_vis', layout: 'layout_vega'});
});

router.get('/choro', function (req, res, next) {
    res.render('vega', {title: 'Choropleth Visualization', script: 'vega_vis_choro', layout: 'layout_vega'});
});

router.get('/horizon', function (req, res, next) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getDailyServiceRatesForCategory("Encampments", results => {
        //TODO: Remove neighborhoods.txt
        res.render('vega_horizon', {
            title: 'Horizon Visualization',
            script: 'vega_vis_horizon',
            layout: 'layout_vega',
            data: 'public/dataset/neighborhoods.txt',
            horizonData: JSON.stringify(results)
        });
    });
});

router.get('/scatter', function (req, res, next) {
    let mysqlClient = new MySqlClient();
    mysqlClient.getMonthlyServiceRates("Castro", results => {
        let data = [results, results];
        res.render('vega_scatter', {
            title: 'Crime and service correlations',
            script: 'vega_vis_scatter',
            layout: 'layout_vega',
            stylesheets: ["style_graph"],
            data: data
        });
    });

    //TODO: Get from MySQL
    // let service = [{"neighborhood": "Castro", "service": 5, "service2": 10},
    //     {"neighborhood": "Other", "service": 4, "service2": 2},
    //     {"neighborhood": "AndMe", "service": 1, "service2": 5},
    //     {"neighborhood": "HelloWorld", "service": 9, "service2": 12},
    //     {"neighborhood": "Cool", "service": 2, "service2": 6},
    //     {"neighborhood": "Name", "service": 17, "service2": 17}];
    // let crime = [{"neighborhood": "Castro", "crime": 8},
    //     {"neighborhood": "Other", "crime": 6},
    //     {"neighborhood": "AndMe", "crime": 1},
    //     {"neighborhood": "HelloWorld", "crime": 4},
    //     {"neighborhood": "Cool", "crime": 15},
    //     {"neighborhood": "Name", "crime": 10}];
    //
    // let data = [service, crime];
    //
});

module.exports = router;
