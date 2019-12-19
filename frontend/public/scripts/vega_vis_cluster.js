//Data for graph
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);

let spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "width": 720,
    "height": 720,
    "datasets": {
        "clusters": data
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
                    "name": "clusters"
                },
                "key": "neighborhood",
                "fields": ["cluster"]
            }
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

vegaEmbed('#graphArea', spec);
