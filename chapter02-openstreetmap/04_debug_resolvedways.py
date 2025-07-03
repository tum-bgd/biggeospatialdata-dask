import pandas as pd
import shapely
import sys


if __name__=="__main__":
    df = pd.read_parquet(sys.argv[1]).set_index("ids")
    print(df[['wkb_hex']])
    
