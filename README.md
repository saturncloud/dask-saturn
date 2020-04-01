# dask-saturn
Python library for interacting with [Dask](https://dask.org/) clusters in
[Saturn Cloud](https://www.saturncloud.io/).

Dask-Saturn mimics the API of
[Dask-Kubernetes](https://github.com/dask/dask-kubernetes), but allows the user
to interact with clusters created within
[Saturn Cloud](https://www.saturncloud.io/).

## Start cluster
In order to interact with a Dask cluster, the cluster must first be created in
the Saturn User Interface. Then, from within a Jupyter notebook, you can start
the cluster and adjust the number of workers.

```python
from dask_saturn import SaturnCluster

cluster = SaturnCluster
cluster
```

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
