var vlSpec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
  "width": 720,
  "height": 720,
  "data": {
    "url": "../dataset/SFFind_neighborhoods.topojson",
    "format": {
      "type": "topojson",
      "feature": "SFFind_neighborhoods"
    }
  },
  "transform": [{
          "calculate": "datum.properties.name", "as": "name"
        },
        {
    "lookup": "name",
    "from": {
      "data": {
        "url": "../dataset/rate.csv"
      },
      "key": "name",
      "fields": ["rate"]
    }
  }],
  "projection": {
    "type": "albersUsa"
  },
  "mark": "geoshape",
  "encoding": {
    "color": {
      "field": "rate",
      "type": "quantitative"
    }
  }
  };

  // Embed the visualization in the container with id `vis`
  vegaEmbed('#vis', vlSpec);