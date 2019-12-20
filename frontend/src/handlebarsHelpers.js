var fs = require('fs');

helpers = {
    //Add handlebars helpers here as functions, eg:
    toCaps: function (value) {
        return value.toUpperCase()
    },

    readData: function (file) {
        let contents = fs.readFileSync(file, 'utf-8');
        contents = contents.split(',');

        return contents;
    },

    formatNames: function (names) {
        temp = JSON.parse(JSON.stringify(names));
        return temp.map(x => x.neighborhood);
    },

    jsonStringify: function (json) {
        return JSON.stringify(json);
    },
    
    getCategories: function (obj) {
        const excludes = ["neighborhood", "month"];
        let allPropertyNames = Object.getOwnPropertyNames(obj);
        return allPropertyNames.filter(n => !excludes.includes(n));
    },

    getIndex: function (arr, index) {
        return arr[index];
    }
};

module.exports.helpers = helpers;
