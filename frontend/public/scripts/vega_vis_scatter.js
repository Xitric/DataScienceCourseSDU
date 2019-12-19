//Data for graph
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);
let serviceData = data[0];
let crimeData = data[1];

//Filters for graph
let serviceCategories = [];
let crimeCategories = [];

const spec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "description": "A scatterplot showing service cases and incident reports for various neighborhoods in San Francisco.",
    "datasets": {
        "service": serviceData,
        "crime": crimeData
    },
    "data": {
        "name": "service"
    },
    "transform": [{
        "lookup": "neighborhood",
        "from": {
            "data": {
                "name": "crime"
            },
            "key": "neighborhood",
            "fields": ["crime"]
        }
    }],
    "repeat": {
        "row": serviceCategories,
        "column": crimeCategories
    },
    "spec": {
        "width": 150,
        "height": 150,
        "mark": "point",
        "encoding": {
            "x": {
                "field": {"repeat": "column"},
                "type": "quantitative"
            },
            "y": {
                "field": {"repeat": "row"},
                "type": "quantitative"
            }
        }
    }
};

function serviceFilterChanged(checkbox) {
    handleCheck(serviceCategories, checkbox.checked, checkbox.value);
}

function crimeFilterChanged(checkbox) {
    handleCheck(crimeCategories, checkbox.checked, checkbox.value);
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

    if (serviceCategories.length > 0 && crimeCategories.length > 0) {
        embedGraph();
    } else {
        removeGraph()
    }
}

function embedGraph() {
    vegaEmbed('#graphArea', spec);
}

function removeGraph() {
    document.getElementById("graphArea").innerHTML = "<p>Please choose at least one category from both service and crime</p>";
}

removeGraph();