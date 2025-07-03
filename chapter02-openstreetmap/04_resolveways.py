""" Resolves a way to a WKB column in parquet """

import pandas as pd
import sys
from pathlib import Path
import shapely
import numpy as np
import os
from tqdm import tqdm



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
    
    
def way2pq(wayfile):
    tag = wayfile.split("-")[1].split(".")[0]
    nodefile=Path("nodes-%s.pq"%(tag))
    ways = pd.read_parquet(wayfile)
    nodes = pd.read_parquet(nodefile).set_index("ids")
    ways = ways.set_index('tags')
    print("Unfiltered:",ways.shape)
    ways = ways.filter(like="highway", axis=0)
    print("Filtered:",ways.shape)
    
    lss = []
    for index,way in tqdm(ways.iterrows()):
        refs = way[["refs"]].to_numpy().flatten()[0]
        ls = resolve_refstring(refs,nodes)
        if type(ls) == list:
            print(f"Warning: Not all points found in {index}")
            lss.append("")
        else:
            lss.append(ls.wkb_hex)
            
    # Prepare a column of wkb
    print("Preparing to write out")
    ways[["wkb_hex"]] = pd.Series(lss)
    ways.to_parquet("ways-resolved-%s.pq" %(tag))
    

if __name__=="__main__":
    way2pq(sys.argv[1])
    
