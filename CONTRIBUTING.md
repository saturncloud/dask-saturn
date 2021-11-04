## Releasing

This section describes how to release a new version of `dask-saturn` to PyPi. It is intended only for maintainers.

1. Created and push a tag on `main` or `release-*` branch:
  
  ```
  git tag -a 0.0.3 -m "Release 0.0.3"
  git push --tags
  ```

2. [Create a new release using the GitHub UI](https://github.com/saturncloud/prefect-saturn/releases/new)
    - the tag should be a version number, like `0.0.3`
    - choose the target from "recent commits", and select the most recent commit on `main`
3. Once this release is created, a GitHub Actions build will automatically start. That build publishes a release to PyPi. You can access logs for this build on the "Actions" tab in GitHub.
