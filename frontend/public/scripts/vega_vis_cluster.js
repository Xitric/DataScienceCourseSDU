//Data for graph
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);
let serviceCategories = [];
let incidentCategories = [];

let spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "width": 720,
    "height": 720,
    "datasets": {
        "clusters": data
    },
    "data": {
        "url": "/dataset/SFFind_neighborhoods.topojson",
        "format": {
            "type": "topojson",
            "feature": "SFFind_neighborhoods"
        }
    },
    "transform": [
        {
            "calculate": "datum.properties.name", "as": "name"
        },
        {
            "lookup": "name",
            "from": {
                "data": {
                    "name": "clusters"
                },
                "key": "neighborhood",
                "fields": ["cluster"]
            }
        },
        {
            "calculate": "datum.cluster === null ? 0 : datum.cluster", "as": "cluster"
        }],
    "mark": "geoshape",
    "encoding": {
        "color": {"field": "cluster", "type": "nominal", "scale": {
                "scheme": "lightgreyteal"
            }
        },
        "tooltip": [
            {"title": "Neighborhood", "field": "name", "type": "nominal"},
            {"title": "Cluster", "field": "cluster", "type": "nominal"}
        ]
    }
};

function serviceFilterChanged(checkbox) {
    handleCheck(serviceCategories, checkbox.checked, checkbox.value);
}

function crimeFilterChanged(checkbox) {
    handleCheck(incidentCategories, checkbox.checked, checkbox.value);
}

function handleCheck(array, checked, value) {
    if (checked) {
        array.push(value);
    } else {
        let index = array.indexOf(value);
        if (index > -1) {
            array.splice(index, 1);
        }
    }
}

vegaEmbed('#graphArea', spec);
