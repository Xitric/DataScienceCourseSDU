const fs = require('fs');
const path = require('path');
const WebHdfs = require('webhdfs');

let hdfs = WebHdfs.createClient({
    host: "namenode",
    port: 9870
});

function upload(local, name, type, onComplete) {
    let localStream = fs.createReadStream(local);
    let remoteStream;

    if (type === "data") {
        remoteStream = hdfs.createWriteStream("/datasets/" + name);
    } else if (type === "app") {
        remoteStream = hdfs.createWriteStream("/apps/" + name);
    } else {
        return;
    }

    localStream.pipe(remoteStream);

    remoteStream.on("error", function (err) {
        console.log(err);
    });

    remoteStream.on("finish", function () {
        onComplete()
    });
}

module.exports = upload;
