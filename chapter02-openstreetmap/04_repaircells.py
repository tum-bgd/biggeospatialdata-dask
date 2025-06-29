import pandas as pd
import sys
from pathlib import Path
import shapely
import numpy as np
import os
from bgdbloomfilter import bloomfilter
from tqdm import tqdm

class cfg:
    k = 3
    m = 8*1024*1024



def resolve_refstring(refs,nodes):
    missing = []
    refs = list(map(int,refs.split(",")))
    all_coords=[]
    ## this could be optimized, for examlpe, by turning nodes into a dict
    for ref in refs:
        coords=None
        try:
            coords = [nodes.loc[ref][["lon"]].to_numpy().flatten()[0],nodes.loc[ref][["lat"]].to_numpy().flatten()[0]]
            all_coords.append(coords)
        except KeyError as e:
            missing.append(ref)
    if len(missing) > 0:
        return missing
    else:
        return shapely.LineString(np.array(all_coords).reshape(-1,2))
    


def resolve_from_nodefile(tag, missing):
    print(f"====== {tag}")
    print(missing.shape)
    source = Path("nodes-%s.pq" %(tag))
    if source.exists():
        nodes = pd.read_parquet(source)
        ids = nodes[["ids"]].to_numpy()
        func = np.vectorize(lambda x: x in missing.flatten())
        choice = func(ids)
        print(np.sum(choice))
        subdf = nodes[choice]
        print(subdf)
        print("Resolving from ", nodes.count())
        print("Found: %d out of %d", subdf.count())
        return subdf
#        testdata = np.unique(subdf[["ids"]].to_numpy(),return_counts=True)
#        print(testdata)
#        sys.exit(0)

    else:
        print(f"cannot resolve as {souce} does not exist")
    
def repair_incompleteways(wayfile):
    tag = wayfile.split("-")[1].split(".")[0]
    nodefile=Path("nodes-%s.pq"%(tag))
    ways = pd.read_parquet(wayfile)
    nodes = pd.read_parquet(nodefile).set_index("ids")
    print("PRocessing %d ways from %d nodes" % (ways.shape[0],nodes.shape[0]))
    
    allmissing = []
    for index,way in tqdm(ways.iterrows()):
        refs = way[["refs"]].to_numpy().flatten()[0]
        ls = resolve_refstring(refs,nodes)
        if type(ls) == list:
            #print(f"Error in {index}")
            allmissing = allmissing + ls
        else:
            pass#print(ls)
    allmissing = np.unique(np.array(allmissing)).reshape(-1,1)
    np.sort(allmissing)
    ## read all BFs
    bfs = dict() # todo: support pickle for BF
    for f in os.listdir("."):
        if f.endswith(".bf"):
            bf = bloomfilter()
            bf.from_bytes(open(f,"rb").read(),cfg.k,cfg.m)
            h = f.split("-")[1].split(".")[0]
            bfs[h] = bf

    ## for each BF find all you could resolve and do so
    for key, bf in bfs.items():
        found = bf.test(allmissing)
        if np.any(found):
            print("key %s is a candidate for %d" % (key,np.sum(found)))
            resolved = resolve_from_nodefile(key, allmissing[found,:])
            print(resolved)
            resolved.to_parquet(nodefile,engine='fastparquet', append=True) # add partition_cols



if __name__=="__main__":
    repair_incompleteways(sys.argv[1])
    
