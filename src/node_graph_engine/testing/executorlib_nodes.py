from __future__ import annotations

from typing import Annotated

from node_graph import dynamic, task
from node_graph.socket_spec import namespace as ns


@task()
def double(x: float) -> float:
    return x * 2


@task.graph(outputs=ns(final=float))
def double_chain(x: float):
    first = double(x=x)
    second = double(x=first.result)
    return {"final": second.result}


@task()
def generate_square_numbers(
    n: int,
) -> Annotated[dict, dynamic(int)]:
    return {f"square_{i}": i**2 for i in range(n)}


@task()
def add_multiply(
    data: Annotated[dict, ns(x=int, y=int)],
) -> Annotated[dict, ns(sum=int, product=int)]:
    return {"sum": data["x"] + data["y"], "product": data["x"] * data["y"]}


@task.graph(
    outputs=ns(
        square=generate_square_numbers.outputs,
        add_multiply=add_multiply.outputs,
    )
)
def composed(
    data: Annotated[dict, add_multiply.inputs.data],
):
    out1 = add_multiply(data=data)
    square_numbers = generate_square_numbers(out1["sum"])
    out2 = add_multiply(data={"x": out1["sum"], "y": out1["product"]})
    return {"square": square_numbers, "add_multiply": out2}
