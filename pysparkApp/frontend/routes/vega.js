let vegaLite = require('vega-lite');

let express = require('express');
let router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('vega', { title: 'Vega Visualization', layout:'layout-vega' });
});

module.exports = router;
