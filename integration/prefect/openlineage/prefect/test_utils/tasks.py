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
def inc(b):
    return b + 1


@task(state_handlers=[])
def multiply(c):
    """
    Multiple the value
    """
    return c * 2


with Flow("test") as test_flow:
    p = Parameter("p")
    g = get(p)
    i = inc(g)
    m = multiply(i)

flow_lock = {}
