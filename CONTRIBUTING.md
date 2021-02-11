## Releases

Releases are created by pushing a tag on `main` or `release-*` branch:

```
git tag -a 0.0.3 -m "Release 0.0.3"
git push --tags
```

This kicks off a GitHub Action which automatically publishes the package to PyPi. You can access logs for this run on the "Actions" tab in GitHub.
