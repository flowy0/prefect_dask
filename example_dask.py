from prefect import task, Flow
import datetime
import random
from time import sleep
from prefect.engine.executors import DaskExecutor
from prefect import Client

@task
def inc(x):
    sleep(random.random() / 10)
    return x + 1


@task
def dec(x):
    sleep(random.random() / 10)
    return x - 1


@task
def add(x, y):
    sleep(random.random() / 10)
    return x + y


@task(name="sum")
def list_sum(arr):
    return sum(arr)


with Flow("dask-example") as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)

# Create a project
client=Client()
client.create_project("dask")
# Register the flow under the project
flow.register(project_name="dask")

# Run the Job under Dask
executor = DaskExecutor(address="tcp://192.168.1.232:8786")
flow.run(executor=executor)