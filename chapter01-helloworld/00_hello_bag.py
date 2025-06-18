import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

from dask.distributed import Client


# a predicate to be used as a "map" function
def isprime(x):
    for i in range(2,x):
        if x % i == 0:
            return False
    return True


#for i in range(10):
#    print(i,isprime(i))


# create a delayed bag of work
import time
if __name__=="__main__":
    client = Client()
    print("Dashboard: %s" %(client.dashboard_link))
    t = time.time()
    b = db.from_sequence(range(100000), npartitions=256)
    c = db.zip(b, b.map(lambda x: isprime(x))).filter(lambda x: x[1] == True)
    print(c)
    print(c.dask)
    
    print(c.compute())
    print("Time: %f" %(time.time()-t))
