import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt

def show_nodes(f):
    df = pd.read_parquet(f)
    print(df.head())
    plt.scatter(df[["lon"]],df[["lat"]])


def show_ways(f):
    df = pd.read_parquet(f)
    print(f)
    print(df.head())
    

if __name__=="__main__":
    plot=False
    for f in sys.argv[1:]:
        if (f == "ways.pq"):
            show_ways(f)
        else:
            show_nodes(f)
            plot=True
            
    if plot:
        plt.show()
