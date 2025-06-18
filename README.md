# biggeospatialdata-dask

This repository contains examples for the courses on Big Geospatial Data Management from Technical University Munich.

## Quick Start

As a first step, you might want to create a new environment, install required dependeices, and start exploring the repository.

On Windows, you might want to open a command line in a working copy and run 
```
py -m venv daskenv2
daskenv2\Scripts\activate
py -m pip install -r requirements.txt
```

For Linux, Mac, and Anaconda, you might replace `py` with your way of launching python which should be python or python3. Furthermore, on Linux or conda, you activate an environment
using a slightly different command.

## Overview

This repository currently contains

1.) [Hello Dask](chapter01-helloworld) showing how to use bags and delayed functions to parallelize Python code.
2.) [OpenStreetMap](chapter02-openstreetmap] is concerned with processing OpenStreetMap

## Credits and Thanks
For OSM, we were very happy to see the NFDI4Ing provide a nice package to read the binary protocol buffer format. While this is not heavily optimized, it simplifies lectures such as this one by an order of magnitude as it is simple and works well.