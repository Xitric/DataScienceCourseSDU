//Data for graph
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);
console.log(data);

let spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "width": 720,
    "height": 720,
    "datasets": {
        "rates": data
    },
    "data": {
        "url": "../dataset/SFFind_neighborhoods.topojson",
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
                    "name": "rates"
                },
                "key": "neighborhood",
                "fields": ["rate"]
            },
        },
        {
            "calculate": "datum.rate === null ? 0 : datum.rate", "as": "rate"
        }
    ],
    "mark": "geoshape",
    "encoding": {
        "color": {
            "field": "rate", "type": "quantitative", "scale": {
                "scheme": "lightgreyteal"
            }
        },
        "tooltip": [
            {"title": "Neighborhood", "field": "name", "type": "nominal"},
            {"title": "Rate", "field": "rate", "type": "quantitative"}
        ]
    }
};

vegaEmbed('#graphArea', spec);