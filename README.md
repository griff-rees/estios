# Regional Input-Output Economics

This package combines classic work on Input-Output economics[^leontif] with a flexibility of region types (cities or towns or even [MSOAs](https://data.gov.uk/dataset/2cf1f346-2f74-4c06-bd4b-30d7e4df5ae7/middle-layer-super-output-area-msoa-boundaries)) and transport costs[^wilson] per sector.

To begin we infer UK city Input-Output tables from national Input-Output tables by weighting on city employment data per sector and simply the distance between cities ([as a crow flies](https://en.wikipedia.org/wiki/As_the_crow_flies)).

This is a proof of concept and much more work is needed to reach the level of detail we would like. Suggestions and contributions (preferably GitHub [issues](https://github.com/griff-rees/regional-input-output/issues/new)) are welcome.

## Basic installation

We use `pyproject.toml` with `poetry` at version [>=1.2.0b1](https://github.com/python-poetry/poetry/commit/dca6ff2699a06c0217ed6d5a278fa3146e4136ff) to manage [dependency groups](https://python-poetry.org/docs/master/managing-dependencies/#optional-groups).

With that version of `poetry` simply downloading a `zip` of this package or a `git clone` should be enough to set up a local environment. Below is an example using `poetry`:

```console
$ poetry init
$ poetry add path/to/regional-input-output.zip
```

## Running an interactive visualisation

Once the local `poetry` environment is created running

```console
$ poetry run region-io server --no-auth
```

in the environment folder should run the model for trade flow between 10 English cities and generate a web visualisation available locally at <http://127.0.0.1:8090/uk-cities> for quarters of 2017. The estimates will be calculated on the fly then cached, and should be ready when

```console
INFO:     Uvicorn running on http://127.0.0.1:8090 (Press CTRL+C to quit)
```

is printed in the terminal.

> *Note:* Without `--no-auth` a login interface will appear to access the visualisation. This provides a very thin layer of security to ease testing prior to a public release. Usernames and passwords are managed in a `json` file (by default `user_db.json`). Documentation on that (and much more) to come.

[^leontif]: Leontief, Wassily. Input-Output Economics. Oxford, UNITED STATES: Oxford University Press, Incorporated, 1986. http://ebookcentral.proquest.com/lib/manchester/detail.action?docID=4701165.

[^wilson]: Wilson, A G. ‘A Family of Spatial Interaction Models, and Associated Developments’. Environment and Planning A: Economy and Space 3, no. 1 (1 March 1971): 1–32. https://doi.org/10.1068/a030001.
