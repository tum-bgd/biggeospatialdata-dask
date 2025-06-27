import numpy as np
import pandas as pd
import sys
import shapefile as shp
import os

def pq2shp(f,g):
    df = pd.read_parquet(f)
    with shp.Writer(g) as w:
        w.field('osmid','C')
        #todo: add your interesting tag information here
        for index,row in df.iterrows():
            getcol = lambda x :row[[x]].to_numpy().flatten()[0] 
            w.record(str(getcol("ids")))
            w.point(getcol("lon"),getcol("lat"))
            #print(index)




if __name__=="__main__":
    if len(sys.argv) == 3:
        print("Doing one file")
        pq2shp(sys.argv[1],sys.argv[2])
    else:
        files = [x for x in os.listdir(".") if x.startswith("nodes-") and x.endswith(".pq")]
        for f in files:
            g = f+".shp"
            print(f"{f}==>{g}")
            pq2shp(f,g)
        
        

