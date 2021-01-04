.PHONY: conda-update
conda-update:
	conda env update -n dask-saturn --file environment.yaml

.PHONY: format
format:
	black .

.PHONY: lint
lint:
	flake8 --count .
	black --check --diff .
	pylint dask_saturn/
