import json

from prefect import Flow
from prefect import Parameter
from prefect import task

from openlineage.prefect.test_utils import RESOURCES
from openlineage.prefect.test_utils.memory_result import MemoryResult


memory_result = MemoryResult()


@task(result=memory_result, checkpoint=True, state_handlers=[])
def get(n):
    """
    Get a json file
    """
    filename = f"{RESOURCES}/{n}.json"
    return json.loads(open(filename).read())


@task(result=memory_result, checkpoint=True, state_handlers=[])
def inc(g):
    return g + 1


@task(state_handlers=[])
def multiply(i):
    """
    Multiple the value
    """
    return i * 2


@task()
def non_data_task(m):
    assert m
    return


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)
    non_data_task(m)
