"""
This script shows (in theory) how dask.delayed can be used to have more "pythonic" parallel compute.

However, this does not work for large numbers as each of the tiny function calls is managed by the scheduler as an independent task.
One challenge in big data is to balance between the size of tasks and the number of tasks. 

Hence, below code can only be used if the number of delayed calls is relatively small (e.g., for parallelizing complex tasks).


"""

import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import dask

from dask.distributed import Client
# a predicate to be used as a "map" function

@dask.delayed
def isprime(x):
    for i in range(2,x):
        if x % i == 0:
            return False
    return True



import time
if __name__=="__main__":
    client = Client()
    print("Dashboard: %s" %(client.dashboard_link))
    t = time.time()
    results=list()
    for i in range(100000):
        results.append(isprime(i))
    results = dask.compute(*results)
    # we might want to zip with the range
    print(results)
    print("Time: %f" %(time.time()-t))
