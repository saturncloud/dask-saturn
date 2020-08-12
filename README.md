# dask-saturn
Python library for interacting with [Dask](https://dask.org/) clusters in
[Saturn Cloud](https://www.saturncloud.io/).

Dask-Saturn mimics the API of
[Dask-Kubernetes](https://github.com/dask/dask-kubernetes), but allows the user
to interact with clusters created within
[Saturn Cloud](https://www.saturncloud.io/).

## Start cluster
From within a Jupyter notebook, you can start a cluster:

```python
from dask_saturn import SaturnCluster

cluster = SaturnCluster()
cluster
```

By default this will start a dask cluster with the same settings that you have
already set in the Saturn UI or in a prior notebook.

To start the cluster with a certain number of workers using the `n_workers`
option. Similarly, you can set the `scheduler_size`, `worker_size`, and `worker_is_spot`.

> Note: If the cluster is already running then you can't change the settings.
> Attempting to do so will raise a warning.

## Adjust number of workers
Once you have a cluster you can interact with it via the jupyter
widget, or using the `scale` and `adapt` methods.

For example, to manually scale up to 20 workers:

```python
cluster.scale(20)
```

To create an adaptive cluster that controls its own scaling:

```python
cluster.adapt(minimum=1, maximum=20)
```

## Interact with client
To submit tasks to the cluster, you sometimes need access to the
`Client` object. Instantiate this with the cluster as the only argument:

```python
from distributed import Client

client = Client(cluster)
client
```

## Close cluster

To terminate all resources associated with a cluster, use the
`close` method:

```python
cluster.close()
```

## Change settings

To update the settings (such as `n_workers`, `worker_size`, `worker_is_spot`, `nthreads`) on an existing cluster, use the `reset` method:

```python
cluster.reset(n_workers=3)
```

You can also call this without instantiating the cluster first:

```python
cluster = SaturnCluster.reset(n_workers=3)
```

## Development

Create/update a dask-saturn conda environment:

```sh
make conda-update
```

Set environment variables to run dask-saturn with a local atlas server:

```sh
export BASE_URL=http://dev.localtest.me:8888/
export SATURN_TOKEN=<JUPYTER_SERVER_SATURN_TOKEN>
```
