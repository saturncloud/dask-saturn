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

Use the `shudown_on_close` option to set up a cluster that is tied to the client
kernel. This functions like a regular dask `LocalCluster`, when your jupyter
kernel dies or is restarted, the dask cluster will close.

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

By default, you'll get one worker, but you can change that using the `n_workers` option. Similarly you can override the scheduler and worker hardware settings with `scheduler_size`, `worker_size`. You can display the available size options using `describe_sizes()`:

```python
>>> describe_sizes()
{'medium': 'Medium - 2 cores - 4 GB RAM',
 'large': 'Large - 2 cores - 16 GB RAM',
 'xlarge': 'XLarge - 4 cores - 32 GB RAM',
 '2xlarge': '2XLarge - 8 cores - 64 GB RAM',
 '4xlarge': '4XLarge - 16 cores - 128 GB RAM',
 '8xlarge': '8XLarge - 32 cores - 256 GB RAM',
 '12xlarge': '12XLarge - 48 cores - 384 GB RAM',
 '16xlarge': '16XLarge - 64 cores - 512 GB RAM',
 'g4dnxlarge': 'T4-XLarge - 4 cores - 16 GB RAM - 1 GPU',
 'g4dn4xlarge': 'T4-4XLarge - 16 cores - 64 GB RAM - 1 GPU',
 'g4dn8xlarge': 'T4-8XLarge - 32 cores - 128 GB RAM - 1 GPU',
 'p32xlarge': 'V100-2XLarge - 8 cores - 61 GB RAM - 1 GPU',
 'p38xlarge': 'V100-8XLarge - 32 cores - 244 GB RAM - 4 GPU',
 'p316xlarge': 'V100-16XLarge - 64 cores - 488 GB RAM - 8 GPU'}
```

Here's an example:

```python
cluster = SaturnCluster(
    scheduler_size="large",
    worker_size="2xlarge",
    n_workers=3,
)
client = Client(cluster)
client
```

## Connect from outside of Saturn

To connect to your Dask cluster from outside of Saturn, you need to set two environment variables: ``SATURN_TOKEN`` and ``SATURN_BASE_URL``.

To get the values for these you'll need to go Saturn in your browser. Go to where you want to connect a Dask cluster. There will be a button that says:
"Connect Externally". Clicking that will open a modal with the values for ``SATURN_TOKEN`` and ``SATURN_BASE_URL``

Remember - that token is private so don't share it with anyone! It'll be a something like `351e6f2d40bf4d15a0009fc086c602df`

```sh
export SATURN_BASE_URL="https://app.demo.saturnenterprise.io"
export SATURN_TOKEN="351e6f2d40bf4d15a0009fc086c602df"
```

After you have set the environment variables, you can open a Python session and connect to your Dask cluster just as you would inside of Saturn:

```python
from dask_saturn import SaturnCluster
from distributed import Client

cluster = SaturnCluster()
client = Client(cluster)
client
```

When you are done working with the dask cluster make sure to shut it down:

```python
cluster.close()
```

## Sync files to workers

When working with distributed dask clusters, the workers don't have access to the same file system as your client does. So you will see files in your jupyter server that aren't available on the workers. To move files to the workers you can use the `RegisterFiles` plugin and call `sync_files` on any path that you want to update on the workers.

For instance if you have a file structure like:
```
/home/jovyan/project/
|---- utils/
|   |---- __init__.py
|   |---- hello.py
|
|---- Untitled.ipynb
```

where hello.py contains:

```python
# utils/hello.py
def greet():
    return "Hello"
```

If the code in hello.py changes or you add new files to utils, you'll want to push those changes to the workers. After setting up the `SaturnCluster` and the `Client`, register the `RegisterFiles` plugin with the workers. Then every time you make changes to the files in utils, run `sync_files`. The worker plugin makes sure that any new worker that comes up will have any files that you have synced.

```python
from dask_saturn import RegisterFiles, sync_files

client.register_worker_plugin(RegisterFiles())
sync_files(client, "utils")

# If a python script has changed, restart the workers so they will see the changes
client.restart()

# import the function and tell the workers to run it
from util.hello import greet
client.run(greet)
```

> TIP: You can always check the state of the filesystem on your workers by running `client.run(os.listdir)`

## Development

Create/update a dask-saturn conda environment:

```sh
make conda-update
```

Set environment variables to run dask-saturn with a local atlas server:

```sh
export SATURN_BASE_URL=http://dev.localtest.me:8888/
export SATURN_TOKEN=<JUPYTER_SERVER_SATURN_TOKEN>
```
