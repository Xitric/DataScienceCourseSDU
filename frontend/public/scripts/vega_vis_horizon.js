let filter = "datum";
let dataJson = document.getElementById('graph').innerHTML;
let data = JSON.parse(dataJson);

const vlSpec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "datasets": {
        "horizonData": data
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
                "title": "Neighborhoods",
                "titleOrient": "top",
                "titleFontSize": 24,
                "labelAngle": 0,
                "labelAlign": "left",
                "labelFontSize": 16
            }
        }
    },
    "spec": {
        "width": 400,
        "height": 20,
        "layer": [{
            "mark": {"type": "area", "clip": true, "orient": "vertical"},
            "encoding": {
                "x": {
                    "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                },
                "y": {
                    "field": "rate", "type": "quantitative",
                    "scale": {"domain": [0, 0.5]},
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
                    "calculate": "datum.rate - 0.5",
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
                    "scale": {"domain": [0, 0.5]}
                },
                "opacity": {"value": 0.3}
            }
        },
            {
                "transform": [
                    {
                        "calculate": "datum.rate * -1",
                        "as": "negy"
                    }
                ],
                "mark": {
                    "type": "area", "clip": true, "orient": "vertical",
                    "color": "red"
                },
                "encoding": {
                    "x": {
                        "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                    },
                    "y": {
                        "field": "negy", "type": "quantitative",
                        "scale": {"domain": [0, 0.5]}
                    },
                    "opacity": {"value": 0.5}
                }
            },
            {
                "transform": [
                    {
                        "calculate": "datum.rate * -1 - 0.5",
                        "as": "negny"
                    }
                ],
                "mark": {"type": "area", "clip": true, "orient": "vertical", "color": "red"},
                "encoding": {
                    "x": {
                        "field": "day", "type": "temporal", "timeUnit": "yearmonthdate"
                    },
                    "y": {
                        "field": "negny", "type": "quantitative",
                        "scale": {"domain": [0, 0.5]}
                    },
                    "opacity": {"value": 0.3}
                }
            }]
    },
    "config": {
        "area": {"interpolate": "monotone"}
    }
};

function handleClick(cb) {
    let temp = "";
    if (cb.checked === true) {
        let str = "datum.neighborhood != '" + cb.name + "'";
        let length = str.length;
        let index = filter.indexOf(str);

        // The entry is the only one in the list
        if (length === filter.length) {
            filter = "datum";
        }
        // The entry is at the start of the list
        else if (index === 0) {
            filter = filter.substr(0, index) + filter.substr(index + length + 4);
        }
        // The entry is at the end of the list
        else if (index + length === filter.length) {
            filter = filter.substr(0, index - 4) + filter.substr(index + length);
        }
        // The entry is inside the list
        else {
            filter = filter.substr(0, index - 4) + filter.substr(index + length);
        }

        // The filter always has to filter something
        if (filter.length < 1) {
            filter = "datum";
        }
    } else {
        temp += "datum.neighborhood != '" + cb.name + "'";

        if (filter === "datum") {
            filter = temp;
        } else {
            filter += " && " + temp;
        }
        console.log(temp);
    }

    console.log(filter);

    vlSpec["transform"][0]["filter"] = filter;
    vegaEmbed('#vis', vlSpec);
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

// Embed the visualization in the container with id `vis`
vegaEmbed('#vis', vlSpec);
