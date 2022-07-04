# Installation and quick start guide

A guide for installing this package.

## Basic installation

The package uses `pyproject.toml` with `poetry` for configuration and metadata. With `poetry`, simply downloading a `zip` of this package or a `git clone` is enough to set up a local environment.

Below is an example using `poetry` for managing an isolated local install:

```console
$ poetry init
$ poetry add path/to/regional-input-output.zip
```

## Running an interactive visualisation

Once the local `poetry` environment is created, run:

!!! note inline end
    `--no-auth` disables a login dropdown for the visualisation.

```console
$ poetry run region-io server --no-auth
MAPBOX access token not found in local .env file.
Starting dash server with port 8090 and ip 127.0.0.1 at /uk-cities
Server running on: http://127.0.0.1:8090/uk-cities
Warning: publicly viewable without authentication.
```

in the environment folder[^mapbox] to model trade flow
between 10 English cities, and to generate a web
visualisation available locally at <http://127.0.0.1:8090/uk-cities> for all
quarters of 2017.

The estimates will be calculated on the fly then cached, and should be ready when

```console
INFO:     Uvicorn running on http://127.0.0.1:8090 (Press CTRL+C to quit)
```

is printed in the terminal.

[^mapbox]: The `MAPBOX access token not found in local .env file` warning indicates you aren't using a registered [Mapbox token](https://docs.mapbox.com/help/getting-started/access-tokens/). This shouldn't prevent you from running locally, but you may need one if you wish to run this publicly. The easiest solution is to add a `.env` file with `MAPBOX=THE-TOKEN` in the directory you run in. More details forthcoming.
