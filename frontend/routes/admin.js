let express = require('express');
const multer = require('multer');
const fs = require('fs');
const LivyClient = require('../src/livyClient');
const hdfsUpload = require('../src/hdfsClient');

let router = express.Router();
const upload = multer({dest: 'uploads/'});

router.get('/', function (req, res) {
    res.render('admin', {title: 'Admin panel', stylesheets: ["style_admin"], livy: req.cookies["livySession"]});
});

router.post("/upload", upload.single("file"), function (req, res) {
    let file = req.file.path;
    let name = req.file.originalname;
    let type = req.body.uploadType;
    hdfsUpload(file, name, type, function () {
        fs.unlink(file, function () {
            res.redirect("back");
        });
    });
});

router.post('/ingest', function (req, res) {
    let client = new LivyClient();
    let type = req.body.type;
    let app = "";

    if (type === "service") {
        app = "service_importer";
    } else if (type === "historicalIncidents") {
        app = "historical_importer";
    } else if (type === "modernIncidents") {
        app = "modern_importer";
    } else {
        res.redirect("back");
        return;
    }

    client.batchSubmit(app, [], function (session) {
        res.cookie('livySession', session);
        res.redirect("back");
    });
});

router.post('/submit', function (req, res) {
    let client = new LivyClient();
    let name = req.body.appName;
    client.batchSubmit(name, [], function (session) {
        res.cookie('livySession', session);
        res.redirect("back");
    });
});

router.get('/update', function (req, res) {
    let client = new LivyClient();
    let id = req.cookies["livySession"].id;

    client.batchQuery(id, session => {
        res.cookie('livySession', session);
        res.redirect("back");
    });
});

module.exports = router;
