# Regional Input-Output Economics

This package combines classic work on Input-Output economics[^leontif] with a flexibility of region types (cities or towns or even [MSOAs](https://data.gov.uk/dataset/2cf1f346-2f74-4c06-bd4b-30d7e4df5ae7/middle-layer-super-output-area-msoa-boundaries)) and transport costs[^wilson] per sector.

To begin we infer UK city Input-Output tables from national Input-Output tables by weighting on city employment data per sector and simply the distance between cities ([as a crow flies](https://en.wikipedia.org/wiki/As_the_crow_flies)).

This is a proof of concept and much more work is needed to reach the level of detail we would like. Suggestions and contributions (preferably GitHub [issues](https://github.com/griff-rees/regional-input-output/issues/new)) are welcome.

## Basic installation

We use `pyproject.toml` with `poetry` at to manage [dependencies](https://python-poetry.org/docs/dependency-specification/).

With that version of `poetry` simply downloading a `zip` of this package or a `git clone` should be enough to set up a local environment. Below is an example using `poetry`:

```console
$ poetry init
$ poetry add path/to/regional-input-output.zip
```

## Running an interactive visualisation

Once the local `poetry` environment is created, run

```console
$ poetry run region-io server --no-auth
MAPBOX access token not found in local .env file.
Starting dash server with port 8090 and ip 127.0.0.1 at /uk-cities
Server running on: http://127.0.0.1:8090/uk-cities
Warning: publicly viewable without authentication.
```

in the environment folder[^mapbox] to model trade flow between 10 English cities to generate a web visualisation available locally at <http://127.0.0.1:8090/uk-cities> for quarters of 2017. The estimates will be calculated on the fly then cached, and should be ready when

```console
INFO:     Uvicorn running on http://127.0.0.1:8090 (Press CTRL+C to quit)
```

is printed in the terminal.

> *Note:* Without `--no-auth` a login dropdown will appear prior to the visualisation. This provides a very thin layer of security to ease testing prior to a public release. Usernames and passwords are managed in a `json` file (by default `user_db.json`). Documentation on that (and much more) to come.

[^leontif]: Leontief, Wassily. Input-Output Economics. Oxford, UNITED STATES: Oxford University Press, Incorporated, 1986. http://ebookcentral.proquest.com/lib/manchester/detail.action?docID=4701165.

[^wilson]: Wilson, A G. ‘A Family of Spatial Interaction Models, and Associated Developments’. Environment and Planning A: Economy and Space 3, no. 1 (1 March 1971): 1–32. https://doi.org/10.1068/a030001.

[^mapbox]: The `MAPBOX access token not found in local .env file` warning indicates you aren't using a registered [Mapbox token](https://docs.mapbox.com/help/getting-started/access-tokens/). This shouldn't prevent you from running locally, but you may need one if you wish to run this publicly. The easiest solution is to add a `.env` file with `MAPBOX=THE-TOKEN` in the directory you run in. More details forthcoming.
