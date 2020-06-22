## Releases

Releases are created by pushing a tag on `main` or `release-*` branch:

```
git tag -a 0.0.3 -m "Release 0.0.3"
git push --tags
```

This kicks off a GitHub Action which automatically publishes the package to PyPi.

When that completes, grab the hash from [pypi](https://pypi.org/project/dask-saturn/#files)
and open a PR to bump that and the version in the conda recipe.

As of writing this, building and uploading the conda package is still a manual process
that @hhuuggoo does.
