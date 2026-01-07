from node_graph import task


@task()
def add(x, y):
    return x + y


@task.graph()
def Fibonacci(target_index, prev=0, curr=1, step=0, increment=1):
    if step > target_index:
        return prev
    next_value = add(x=prev, y=curr).result
    next_step = add(step, increment).result
    return Fibonacci(
        target_index=target_index,
        prev=curr,
        curr=next_value,
        step=next_step,
        increment=increment,
    )