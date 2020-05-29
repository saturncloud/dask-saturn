.PHONY: conda-update
conda-update:
	envsubst < environment.yaml > /tmp/environment.yaml
	conda env update -n dask-saturn --file /tmp/environment.yaml