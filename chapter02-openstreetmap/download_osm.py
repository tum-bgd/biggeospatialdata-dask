import numpy as np
from matplotlib import pyplot as plt

def download_example():
    import os, urllib.request
    if not os.path.exists('andorra.osm.pbf'):
        filename, headers = urllib.request.urlretrieve('https://download.geofabrik.de/europe/andorra-190101.osm.pbf',
        filename='andorra.osm.pbf'
     )


if __name__=="__main__":
    download_example()
    import esy.osm.pbf
    osm = esy.osm.pbf.File('andorra.osm.pbf')
    # let us collect all points
    lonlat = []
    for i,entry in enumerate(osm):
        if type(entry) == esy.osm.pbf.file.Node:
            lonlat.append([*entry.lonlat])
        

    lonlat = np.array(lonlat)
    print(lonlat)
    np.save("andorra.npy",lonlat)

#    plt.scatter(lonlat[:,0],lonlat[:,1])
#    plt.show()
    #len([entry for entry in osm if entry.tags.get('leisure') == 'park'])
