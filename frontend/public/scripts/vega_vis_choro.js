//Data for graph
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);

let spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "width": 720,
    "height": 720,
    "datasets": {
        "rates": data
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

// Controls the radio buttions
function typeChanged(radio) {
    if (radio.value === "service") {
        document.getElementById("incidentCategory").setAttribute("disabled", "disabled");
        document.getElementById("incidentCategory").setAttribute("hidden", "hidden");
        document.getElementById("serviceCategory").removeAttribute("disabled");
        document.getElementById("serviceCategory").removeAttribute("hidden");
    } else if (radio.value === "incident") {
        document.getElementById("incidentCategory").removeAttribute("disabled");
        document.getElementById("incidentCategory").removeAttribute("hidden");
        document.getElementById("serviceCategory").setAttribute("disabled", "disabled");
        document.getElementById("serviceCategory").setAttribute("hidden", "hidden");
    }
}

vegaEmbed('#graphArea', spec);
