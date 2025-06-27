"""

additional dependency:
libgeohash
geopandas
"""

import numpy as np
import pandas as pd
import sys
import shapefile as shp
import os
#w = shapefile.Writer('shapefiles/test/polygon')
#>>> w.field('name', 'C')
#
#>>> w.poly([
#...	        [[113,24], [112,32], [117,36], [122,37], [118,20]], # poly 1
#...	        [[116,29],[116,26],[119,29],[119,32]], # hole 1
#...         [[15,2], [17,6], [22,7]]  # poly 2
#...        ])
#>>> w.record('polygon1')
#
#>>> w.close()
#def pq2shp(f,g):
#    df = pd.read_parquet(f)
#    with shp.Writer(g) as w:
#        w.field('osmid','C')
#        #todo: add your interesting tag information here
#        for index,row in df.iterrows():
#            getcol = lambda x :row[[x]].to_numpy().flatten()[0] 
#            w.record(str(getcol("ids")))
#            w.point(getcol("lon"),getcol("lat"))
#            #print(index)
#


import libgeohash as gh
import geopandas as gpd
from shapely.geometry import Polygon

if __name__=="__main__":
    files = [x for x in os.listdir(".") if x.startswith("nodes-") and x.endswith(".pq")]
    lengths = [int(pd.read_parquet(f).count()[["ids"]].to_numpy().flatten()[0]) for f in files]
    tags = [f.split("-")[1].split(".")[0] for f in files]
    print(lengths)
    print(tags)

    geoms = [gh.geohash_to_polygon([t]) for t in tags]
    # add crs using wkt or EPSG to have a .prj file
    gdr = gpd.GeoDataFrame({'files': files, 'lengths':lengths,'tags':tags,'geometry':geoms} , crs='EPSG:4326')
    print(gdr.head())
    gdr.to_file("nodes-stats.shp")
