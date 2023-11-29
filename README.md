# Economic Spatial Temporal Input-Output Systems: ESTIOS
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10218781.svg)](https://zenodo.org/badge/latestdoi/10218779)

This package combines classic work on Input-Output economics[^leontif] with transport costs[^wilson] and changes over time to estimate trade between different geographic regions (cities or towns or even [MSOAs](https://data.gov.uk/dataset/2cf1f346-2f74-4c06-bd4b-30d7e4df5ae7/middle-layer-super-output-area-msoa-boundaries)).

To begin we infer UK city Input-Output tables from national Input-Output tables by weighting on city employment data per sector and simply the distance between cities ([as a crow flies](https://en.wikipedia.org/wiki/As_the_crow_flies)).

This is a proof of concept and much more work is needed to reach the level of detail we would like. Suggestions and contributions (preferably GitHub [issues](https://github.com/griff-rees/estios/issues/new)) are welcome.

## Basic installation

We use [geopandas](https://geopandas.org/en/stable/) and it will be necessary to install some system pacakges to manage this. [`GDAL`](https://gdal.org/) is often the main source of difficulty. Via `debian` `linux` distros you should be able to simply install the library without modificaitons. On `macOS` via [`homebrew`](https://formulae.brew.sh/formula/gdal) you can install that with

```console
$ brew install gdal
```
See [`fiona`](https://fiona.readthedocs.io/en/latest/README.html#installation) and [geopandas documentation](https://geopandas.org/en/stable/getting_started/install.html#installing-with-pip) for more information, and feel free to raise a ticket.

We use `pyproject.toml` with `poetry` to manage [dependencies](https://python-poetry.org/docs/dependency-specification/).

With `poetry` version `>=1.2` simply downloading a `zip` of this package or a `git clone` should be enough to set up a local environment. Below is an example using `poetry`:

```console
$ poetry init
$ poetry add path/to/estios.zip
```

## Running an interactive visualisation

Once the local `poetry` environment is created, run

```console
$ poetry run estios server --no-auth
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

# Copyright

## Software

Copyright 2022 The Alan Turing Institute, British Library Board, Queen Mary University of London, University of Exeter, University of East Anglia and University of Cambridge.

See [LICENSE](LICENSE) for more details.

## Data

This repo contains and uses data from the UK Office of National Statistics under the [Open Governance License](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) and the [Organisation for Economic Co-operation and Development (OECD)](https://www.oecd.org/termsandconditions/)

# Funding and Acknowledgements

This software has been developed as part of two UK Research and Innovation (UKRI) Strategic Priorities Funded projects:

- [AI for Science and Governance (ASG)](https://www.turing.ac.uk/research/asg), specifically Wave 1 of ASG under the Engineering and Physical Sciences Research Council (EPSRC), specifically the *Digital Twins: Urban Analytics* theme. Grant reference: EP/W006022/1
- [Living with Machines](https://livingwithmachines.ac.uk), funded under the Arts and Humanities Research Council (AHRC), with The Alan Turing Institute, the British Library and the Universities of Cambridge, East Anglia, Exeter, and Queen Mary University of London. Grant reference: AH/S01179X/1

We are grateful for the extensive advice and support from colleagues across both projects.

[^leontif]: Leontief, Wassily. Input-Output Economics. Oxford, UNITED STATES: Oxford University Press, Incorporated, 1986. http://ebookcentral.proquest.com/lib/manchester/detail.action?docID=4701165.

[^wilson]: Wilson, A G. ‘A Family of Spatial Interaction Models, and Associated Developments’. Environment and Planning A: Economy and Space 3, no. 1 (1 March 1971): 1–32. https://doi.org/10.1068/a030001.

[^mapbox]: The `MAPBOX access token not found in local .env file` warning indicates you aren't using a registered [Mapbox token](https://docs.mapbox.com/help/getting-started/access-tokens/). This shouldn't prevent you from running locally, but you may need one if you wish to run this publicly. The easiest solution is to add a `.env` file with `MAPBOX=THE-TOKEN` in the directory you run in. More details forthcoming.
