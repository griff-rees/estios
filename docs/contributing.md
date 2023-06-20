# Developing

For the examples below commands are run within a `poetry run` prefix to avoid ambiguity between environments.

## Running tests

Assuming a local `git checkout` run

```console
$ poetry run pytest
```

### Running xfail tests

To run tests that are marked `xfail`:

```console
$ poetry run pytest --runxfail
```

!!! note

    It's worth using `--runxfail` a lot as issues can linger for quite a while.
