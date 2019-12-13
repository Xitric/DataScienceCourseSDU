let express = require('express');
let router = express.Router();
const SparkClient = require('../src/sparkClient');

/* GET admin panel. */
router.get('/', function (req, res, next) {
    res.render('admin', {title: 'Admin panel', stylesheets: ["style_admin"]});
});

router.post('/ingest', function (req, res, next) {
    let client = new SparkClient();
    client.ingestDataStreaming("Hello");
    res.redirect("back");
});

module.exports = router;
