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

    formatCorr: function (obj) {
        let json = JSON.parse(JSON.stringify(obj));
        let obje = {};
        for (let j = 0; j < json.length; j++) {
            const arr = json[j];
            
            let keys = Object.keys(arr);
            let val = Object.values(arr);
            let values = {};
            for (let i = 1; i < keys.length; i++) {
                values[keys[i]] = val[i];
            }
            obje[val[0]] = values;
        }

        let keys = Object.keys(obje)
        let inner_keys = Object.keys(obje[keys[0]])
        
        let table = []
        let row = [""];
        for (let i = 0; i < inner_keys.length; i++) {
            const element = inner_keys[i];
            row.push(element);
        }
        table.push(row);
        

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            row = [key];

            for (let j = 0; j < inner_keys.length; j++) {
                const val = obje[keys[i]][inner_keys[j]];
                row.push(val);
            }
            table.push(row);
        }

        return table;
    },

    getIndex: function (arr, index) {
        return arr[index];
    }
};

module.exports.helpers = helpers;
