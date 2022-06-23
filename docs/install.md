# Installation and quick start guide

A guide for installing this package.

## Basic installation

The package uses `pyproject.toml` with `poetry` at version [>=1.2.0b1](https://github.com/python-poetry/poetry/commit/dca6ff2699a06c0217ed6d5a278fa3146e4136ff) to manage [dependency groups](https://python-poetry.org/docs/master/managing-dependencies/#optional-groups).

With version `1.2.0b1` of `poetry` simply 
downloading a `zip` of this package or a `git clone` is enough to set up a local environment. 

Below is an example using `poetry`:

```console
$ poetry init
$ poetry add path/to/regional-input-output.zip
```

## Running an interactive visualisation

Once the local `poetry` environment is created, run:

```console
$ poetry run region-io server --no-auth
MAPBOX access token not found in local .env file.
Starting dash server with port 8090 and ip 127.0.0.1 at /uk-cities
Server running on: http://127.0.0.1:8090/uk-cities
Warning: publicly viewable without authentication.
```

in the environment folder<sup>[mapbox]</sup> to model trade flow 
between 10 English cities, and to generate a web 
visualisation available locally at <http://127.0.0.1:8090/uk-cities> for all 
quarters of 2017. 

The estimates will be calculated on the fly then cached, and should be ready when

```console
INFO:     Uvicorn running on http://127.0.0.1:8090 (Press CTRL+C to quit)
```

is printed in the terminal.

<span style="color:red"> 
*Note:* Without `--no-auth` a login dropdown will appear prior to the visualisation. 
This provides a very thin layer of security to ease testing prior to a public release.
 Usernames and passwords are managed in a `json` file (by default `user_db.json`).
</span>

---

<sup>[mapbox]</sup>: 
The `MAPBOX access token not found in local .env file` warning indicates you aren't using 
a registered [Mapbox token](https://docs.mapbox.com/help/getting-started/access-tokens/). 
This shouldn't prevent you from running locally, but you may need one if you wish to run this publicly. 
The easiest solution is to add a `.env` file with `MAPBOX=THE-TOKEN` in the directory you run in. 
More details forthcoming.

