import numpy as np
import rasterio
import pyproj

def download_example():
    import os, urllib.request
    if not os.path.exists('example.tif'):
        filename, headers = urllib.request.urlretrieve('https://api.bgd.ed.tum.de/datasets/biggeospatialdata-lecture/32855_5412.tif',
        filename='example.tif'
     )

from shapely.ops import transform
from shapely import buffer
import libgeohash as gh

if __name__=="__main__":
    download_example()
    print("Example.tif")
    ds = rasterio.open('example.tif')
    print(type(ds.bounds))
    print(ds.crs)
    print(ds.transform)

    from shapely.geometry import box
    geom = box(*ds.bounds,)
    print(geom.wkt)
    geom = buffer(geom,50) # 50 m larger
    
    from_srid = pyproj.CRS(ds.crs)
    to_srid = pyproj.CRS('EPSG:4326')
    p = pyproj.Transformer.from_crs(from_srid, to_srid, always_xy=True).transform
    geom = transform(p,geom)

    # now find out geohash cell set that covers
    cells = gh.polygon_to_geohash(geom,4)
    print(cells)
    
