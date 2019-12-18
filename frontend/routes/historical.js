let express = require('express');
const HbaseClient = require('../src/hbaseClient');

let router = express.Router();

router.get('/', function (req, res, next) {
    res.render('historical', {title: 'Historical'});
    let hbaseClient = new HbaseClient();
    hbaseClient.testTable();
});

module.exports = router;