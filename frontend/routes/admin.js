let express = require('express');
const multer = require('multer');
const fs = require('fs');
const LivyClient = require('../src/livyClient');
const hdfsUpload = require('../src/hdfsClient');

let router = express.Router();
const upload = multer({dest: 'uploads/'});

router.get('/', function (req, res, next) {
    res.render('admin', {title: 'Admin panel', stylesheets: ["style_admin"], livy: req.cookies["livySession"]});
});

router.post("/upload", upload.single("file"), function (req, res, next) {
    let file = req.file.path;
    let name = req.file.originalname;
    let type = req.body.uploadType;
    hdfsUpload(file, name, type, function () {
        fs.unlink(file, function () {
            console.log("Completed");
            res.redirect("back");
        });
    });
});

router.post('/submit', function (req, res, next) {
    let client = new LivyClient();
    let name = req.body.appName;
    client.batchSubmit(name, [], function (session) {
        console.log(session);
        res.cookie('livySession', session);
        res.redirect("back");
    });
});

router.get('/update', function (req, res, next) {
    let client = new LivyClient();
    let id = req.cookies["livySession"].id;

    if (id) {
        client.batchQuery(id, function (session) {
            console.log(session);
            res.redirect("back");
        });
    } else {
        res.redirect("back");
    }
});


router.post('/ingest', function (req, res, next) {
    let client = new LivyClient();
    let session = client.batchSubmit();
    res.redirect("back");

    // let client = new LivyClient();
    // client.createSession(function (session) {
    //     client.execute(session, "print(sc.parallelize([1,2,3]).count())", function (session) {
    //         console.log(session);
    //         res.cookie('livySession', session);
    //         res.redirect("back");
    //         // client.kill(body, function (body) {
    //         //     console.log(body);
    //         //     res.redirect("back");
    //         // });
    //     });
    // });
});

// router.post('/update', function (req, res, next) {
//     let client = new LivyClient();
//     client.batchQuery();
//     res.redirect("back");
//
//     // let session = req.cookies["livySession"];
//     // let client = new LivyClient();
//     //
//     // client.queryResult(session, function (session) {
//     //     res.cookie('livySession', session);
//     //     console.log(session);
//     //     res.redirect("back");
//     // });
// });

module.exports = router;
