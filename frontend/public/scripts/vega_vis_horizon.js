let filter = "datum";
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);

let dataset = data[0]
let category = data[2]

// Generate the horizon graph schema
function generateGraph(dataset, filter, maxScale) {
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "datasets": {
            "horizonData": dataset
        },
        "data": {
            "name": "horizonData"
        },
        "transform": [
            {"filter": filter}
        ],
        "facet": {
            "row": {
                "field": "neighborhood",
                "type": "nominal",
                "header": {
                    "title": "Neighborhoods" + ": " + category,
                    "titleOrient": "top",
                    "titleFontSize": 24,
                    "labelAngle": 0,
                    "labelAlign": "left",
                    "labelFontSize": 16
                }
            }
        },
        "spec": {
            "width": 600,
            "height": 40,
            "layer": [{
                "mark": {"type": "area", "clip": true, "orient": "vertical"},
                "encoding": {
                    "x": {
                        "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                    },
                    "y": {
                        "field": "rate", "type": "quantitative",
                        "scale": {"domain": [0, maxScale / 3.0]},
                        "axis": {
                            "title": null,
                            "labels": true
                        }
                    },
                    "opacity": {"value": 0.5}
                }
            }, {
                "transform": [
                    {
                        "calculate": "datum.rate - " + maxScale / 3.0,
                        "as": "ny"
                    }
                ],
                "mark": {"type": "area", "clip": true, "orient": "vertical"},
                "encoding": {
                    "x": {
                        "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                    },
                    "y": {
                        "field": "ny", "type": "quantitative",
                        "scale": {"domain": [0, maxScale / 3.0]}
                    },
                    "opacity": {"value": 0.3}
                }
            }, {
                "transform": [
                    {
                        "calculate": "datum.rate - " + maxScale * 2.0 / 3.0,
                        "as": "ny"
                    }
                ],
                "mark": {"type": "area", "clip": true, "orient": "vertical"},
                "encoding": {
                    "x": {
                        "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                    },
                    "y": {
                        "field": "ny", "type": "quantitative",
                        "scale": {"domain": [0, maxScale / 3.0]}
                    },
                    "opacity": {"value": 0.3}
                }
            }]
        },
        "config": {
            "area": {"interpolate": "monotone"}
        }
    };
}

// Find the max rate to scale after
function scaleRates(neighborhoods = null) {
    let maxRate = 0.5;

    if (neighborhoods == null) {
        maxRate = Math.max.apply(Math,dataset.map(function(o){ return o.rate;}));
    }
    else {
        let tempNeigh = dataset.filter(function(o){
            return neighborhoods.indexOf(o.neighborhood) >= 0;
        });
        maxRate = Math.max.apply(Math,tempNeigh.map(function(o){ return o.rate;}));
    }

    return maxRate;
}

// Get all unchecked checkboxes
function getCheckboxes() {
    let checkboxes = document.getElementsByClassName('filterCheck');
    let checkboxesChecked = [];
    // loop over them all
    for (let i=0; i<checkboxes.length; i++) {
       // And stick the checked ones onto an array...
       if (checkboxes[i].checked) {
          checkboxesChecked.push(checkboxes[i].name);
       }
    }
    // Return the array if it is non-empty, or null
    return checkboxesChecked.length > 0 ? checkboxesChecked : null;
}

// Generate the filter from the checked checkboxes
function generateFilter(checked) {
    let currFilter = "";
    let tempArr;

    if (checked == null) {
        currFilter = "!datum"
    }
    else {
        tempArr = checked.slice();
        tempArr.forEach((name, i, array) => {
            array[i] = "datum.neighborhood == '" + name + "'";
        });
        currFilter = tempArr.join(" || ");
    }

    return currFilter;
}

// Receive a filter request from the user
function submitFilters() {
    let checked = getCheckboxes();

    let currFilter = generateFilter(checked);

    let rate = scaleRates(checked);
    vlSpec = generateGraph(dataset, currFilter, rate);
    vegaEmbed('#vis', vlSpec);
}

// Toggles all the checkboxes on or off
function checkAll(checktoggle) {
    var checkboxes = new Array();
    checkboxes = document.getElementsByClassName('filterCheck');

    for (var i = 0; i < checkboxes.length; i++) {
        if (checkboxes[i].type == 'checkbox') {
            checkboxes[i].checked = checktoggle;
        }
    }
}

// Clicking dropdown button will toggle display
function btnToggle() {
    document.getElementById("fiDropdown").classList.toggle("show");
}

// Prevents menu from closing when clicked inside
document.getElementById("fiDropdown").addEventListener('click', function (event) {
    event.stopPropagation();
});

// Closes the menu in the event of outside click
window.onclick = function (event) {
    if (!event.target.matches('.dropbutton')) {

        let dropDowns =
            document.getElementsByClassName("dropdownmenu-content");

        let i;
        for (i = 0; i < dropDowns.length; i++) {
            let openDropdown = dropDowns[i];
            if (openDropdown.classList.contains('show')) {
                openDropdown.classList.remove('show');
            }
        }
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

// Changes the radio button to the currently loaded one
if (data[1] == "incident") {
    radiobtn = document.getElementById("radioInci");
    radiobtn.checked = true;
    typeChanged(radiobtn)
}
else {
    radiobtn = document.getElementById("radioServ");
    radiobtn.checked = true;
    typeChanged(radiobtn)
}

let vlSpec = generateGraph(dataset, filter, scaleRates());

// Embed the visualization in the container with id `vis`
vegaEmbed('#vis', vlSpec);
