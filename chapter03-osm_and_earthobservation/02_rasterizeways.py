"""
Rasterize some of the ways

"""

import sys
import warnings
import pandas as pd
import libgeohash as gh
import numpy as np
import rasterio
import rasterio.features
import pyproj
import shapely
from shapely import buffer
from shapely.ops import transform
from shapely.geometry import box
from matplotlib import pyplot as plt

class cfg:
    lwd=10 # line width in meter, if None, ignored


def individual_feature_generator(df):
    for index, feature in df.iterrows():
        geom = shapely.from_wkb(df.loc[index, "wkb_hex"])        
        if type(geom) == np.ndarray:
            for g in geom:
                yield index, g
        else:
            yield index, geom
                

def feature_generator(df, p,crop_to):
    N=0
    Nskip = 0
    for index, geom in individual_feature_generator(df):
        geom = transform(p,geom)
        N = N+1
        if not crop_to.intersects(geom):
            Nskip = Nskip +1
            continue
        if cfg.lwd is not None:
            geom = buffer(geom, cfg.lwd/2) # enlarge lines to polygons of lwd m width
        value = 250 # here, you can make use of attribs
        yield (geom,value)
    print(f"Exhausted. Skipped {Nskip} out of {N}")



    
def rasterize_ways(filename, outfilename):
    ds = rasterio.open(filename)
    geom = buffer(box(*ds.bounds,),50) # 50 m larger
    
    from_srid, to_srid = pyproj.CRS(ds.crs),pyproj.CRS('EPSG:4326')
    p = pyproj.Transformer.from_crs(from_srid, to_srid, always_xy=True).transform
    cells = gh.polygon_to_geohash(transform(p,geom),4,inner=False)
    print(cells)

    for cell in cells:
        df = pd.read_parquet(f"ways-resolved-{cell}.pq")
        print(df.shape)
        # complete this: we are not reading all, but have only the last

    from_srid, to_srid = pyproj.CRS('EPSG:4326'),pyproj.CRS(ds.crs)
    fg =  feature_generator(df,
                            pyproj.Transformer.from_crs(
                                from_srid,
                                to_srid,
                                always_xy=True).transform,
                            box(*ds.bounds))
    meta = ds.meta.copy()
    with rasterio.open(outfilename, 'w+', **meta) as out:
        out_arr = out.read(1)
        burned = rasterio.features.rasterize(fg, out=out_arr, transform=ds.transform)
#        plt.imshow(burned)
#        plt.colorbar()
#        plt.show()
        for in in [1,2,3]:
            out.write_band(i, burned)
        




if __name__=="__main__":
    rasterize_ways(sys.argv[1], sys.argv[2])
