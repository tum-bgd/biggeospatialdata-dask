""" 
This is the OSM PBF parser for the street network

additional requirements:
pygeohash_fast
h5py
pytable
fastparquet
"""

import numpy as np
from matplotlib import pyplot as plt
import esy.osm.pbf
from tqdm import tqdm
import os, urllib.request
from pygeohash_fast import encode_many
import pandas as pd
from pathlib import Path
import json
import bloomfilter as bf

hash_chars = 4



class NodeMemtable:
    def __init__(self, capacity=10000):
        self.data = []
        self.capacity = capacity
        self.lonlat = []
        
    def insert(self,x):
        self.data.append(x)
        self.lonlat.append([*x.lonlat])
        
        if len(self.data) >= self.capacity:
            self.spillout()
            self.data = []
            self.lonlat = []
    def sync(self):
        self.spillout()
    def spillout(self):
        nodes_coords = np.array(self.lonlat)
        nodes_coords[nodes_coords[:,0] < -180.0,0] = -180.0;
        nodes_coords[nodes_coords[:,0] > 180.0,0] = 180.0;
        nodes_coords[nodes_coords[:,1] < -90.0,1] = -90.0;
        nodes_coords[nodes_coords[:,1] > 90.0,1] = 90.0;
        try:
            hashes = encode_many(nodes_coords[:,0],nodes_coords[:,1],hash_chars)
        except:
            print("ERROR in hashes")

        tags = [json.dumps(x.tags) for x in self.data]
        ids = np.array([x.id for x in self.data],dtype=np.uint64)
        hashes=np.array(hashes)
        slicedbread = pd.DataFrame({"ids":ids,"hashes":hashes, "lon":nodes_coords[:,0], "lat":nodes_coords[:,1], "tags":tags})
        slicedbread = slicedbread.groupby('hashes').apply(lambda x: self.spillout_hash(x),include_groups=True)
        print(slicedbread)
        
        print("Spilling out")

    def spillout_hash(self, x):
        filename = Path("nodes-%s.pq" %(x[["hashes"]].to_numpy().flatten()[0]))
        if (filename.exists()):
            x.to_parquet(filename,engine='fastparquet', append=True) # add partition_cols
        else:
            x.to_parquet(filename,engine='fastparquet') # add partition_cols
            


class WayMemtable:
    """ id, tags, refs """
    def __init__(self, capacity=10000):
        self.data = []
        self.capacity = capacity
        self.refs = []
        self.N=0
        
    def insert(self,x):
        self.data.append(x)
        if len(self.data) >= self.capacity:
            self.spillout()
            self.data = []

    def spillout(self):
        tags = [json.dumps(x.tags) for x in self.data]
        ids = np.array([x.id for x in self.data],dtype=np.uint64)
        refs = [",".join(map(str,x.refs)) for x in self.data ] # CSV for uint64 bit safety not guaranteed in many JSON parsers
        x = pd.DataFrame({"ids":ids,"refs":refs, "tags":tags})
        filename = Path("ways.pq")
        if (filename.exists()):
            x.to_parquet(filename,engine='fastparquet', append=True) # add partition_cols
        else:
            x.to_parquet(filename,engine='fastparquet') # add partition_cols
             
    def sync(self):
        self.spillout()

        

def download_example():
    if not os.path.exists('andorra.osm.pbf'):
        filename, headers = urllib.request.urlretrieve('https://download.geofabrik.de/europe/andorra-190101.osm.pbf',
        filename='andorra.osm.pbf'
     )

def importfile(filename='andorra.osm.pbf'):
    ### Phase 1: Externalize all the nodes
    osm = esy.osm.pbf.File(filename)
    # let us collect all points
    lonlat = []
    nodetable = NodeMemtable()
    waytable = WayMemtable()
    for i,entry in tqdm(enumerate(osm)):
        if type(entry) == esy.osm.pbf.file.Node:
            nodetable.insert(entry)
        if type(entry) == esy.osm.pbf.file.Way:
            waytable.insert(entry)
    # bring all to disk
    nodetable.sync() 
    waytable.sync() 

if __name__=="__main__":
    download_example()
    importfile()
