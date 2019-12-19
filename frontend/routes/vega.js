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
        console.log(results);
        res.render('vega_horizon', {title: 'Horizon Visualization', script: 'vega_vis_horizon', layout: 'layout_vega', data: 'public/dataset/neighborhoods.txt', horizonData: JSON.stringify(results)});
    });
});

module.exports = router;
