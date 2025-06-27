import pandas as pd
import os
import numpy as np
import sys
from pathlib import Path
from bgdbloomfilter import bloomfilter
from dask.distributed import Client
import dask.dataframe as dd

class cfg:
    k = 3
    m = 8*1024*1024


import time

def cellset(c,bfs):
    if type(c) != str:
        return []
    if not "," in c:
        return []
    ret = list()
    c = np.array(list(map(int,c.split(",")))).reshape(-1,1)
    for k,bf in bfs.items():
        if np.any(bf.test(c)):
            ret.append(k)
    return (np.unique(np.array(ret)))
    


def krnl_assign2cell(x):
    bfs = dict() # todo: support pickle for BF
    for f in os.listdir("."):
        if f.endswith(".bf"):
            bf = bloomfilter()
            bf.from_bytes(open(f,"rb").read(),cfg.k,cfg.m)
            h = f.split("-")[1].split(".")[0]
            bfs[h] = bf

    ret = list()
    for row in x.to_dict(orient='records'):
        refs = row["refs"]
        for cell in cellset(refs,bfs):
            out = row.copy()
            out["cell"] = str(cell)
            ret.append(out)
    return pd.DataFrame.from_dict(ret)


def store(x):
    print("Store called with ")
    cell = x[["cell"]].to_numpy()[0,0]
    filename = Path("ways-%s.pq" %(cell))
    if (filename.exists()):
        x.to_parquet(filename,engine='fastparquet', append=True) # add partition_cols
    else:
        x.to_parquet(filename,engine='fastparquet') # add partition_cols
    return 42

if __name__=="__main__":
    # All BFs are there, let us distribute the ways
    client = Client()
    print("Dashboard: %s" %(client.dashboard_link))

    ddf = dd.read_parquet("ways.pq")
    out = ddf.repartition(npartitions=128)
    out = out.map_partitions(krnl_assign2cell, meta={'ids':'u8', 'refs':'U', 'tags':'U' , 'cell':'U'})
    out = out.groupby('cell')[['ids','refs','tags','cell']].apply(store, meta=('N','u8'))
    out = out.compute()
