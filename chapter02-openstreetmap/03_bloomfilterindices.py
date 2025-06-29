import pandas as pd
import os
import numpy as np
import sys
from pathlib import Path
from bgdbloomfilter import bloomfilter

class cfg:
    k = 3
    m = 8*1024*1024

if __name__=="__main__":
    # We assume that each parquet can be held in main memory ==> take a finer spatial hash if needed
    files = [x for x in os.listdir(".") if x.endswith(".pq") and x.startswith("nodes-")]
    for i,f in enumerate(files):
        print(f)
        bf = bloomfilter()
        bfname=Path("%s.bf" %(f))
        if bfname.exists():
            bfdata = open(bfname,"rb").read()
            bf.from_bytes(bfdata,cfg.k,cfg.m)
        else:
            bf.configure(cfg.k,cfg.m)

        x =pd.read_parquet(f,columns=["ids"]).to_numpy()
        print(x.shape)
        bf.insert(x)
        ## save filter
        with open(bfname, "wb") as out:
            out.write(bf.to_bytes())
#        print("%03d\t%s" %(i,bf.to_bytes()))
        
