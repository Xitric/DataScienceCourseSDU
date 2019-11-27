var vlSpec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
    "width": 700,
    "height": 500,
    "config": {
        "view": {
        "stroke": "transparent"
        }
    },
    "layer": [
        {
        "data": {
            "url": "https://data.sfgov.org/resource/6ia5-2f8k.geojson",
            "format": {
                "property": "features"
            }
        },
        "mark": {
            "type": "geoshape",
            "stroke": "white",
            "strokeWidth": 2
        },
        "encoding": {
            "color": {
            "value": "#eee"
            }
        }
        },
        {
          "data": {
            "values": [
              {"lat": 37.795355, "long": -122.421486666667},
              {"lat": 37.766571044922, "long": -122.421531677246}
            ]
          },
          "mark": "point",
          "encoding": {
              "longitude": { "field": "long", "type": "geojson" },
              "latitude": { "field": "lat", "type": "geojson" }
            }
        }
    ]
  };

  // Embed the visualization in the container with id `vis`
  vegaEmbed('#vis', vlSpec);