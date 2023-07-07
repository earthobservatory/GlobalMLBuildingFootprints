"""
This snippet demonstrates how to access and convert the buildings
data from .csv.gz to geojson with AOI defined for use in common GIS tools. You will
need to install pandas, geopandas, mercantile and shapely.
conda create --name global_footprints
conda activate global_footprints
conda install pandas geopandas shapely tqdm fiona mercantile
"""

import pandas as pd
import geopandas as gpd
import shapely.geometry
from tqdm import tqdm
import mercantile
import fiona
import os
import shutil
import argparse
import json


def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Fetch Microsoft Bing building footprints dataset.')
    parser.add_argument('-a', '--aoi', dest='aoi_file', type=str, default='',
            help='GeoJSON file of AOI required. Create one via geojson.io and save to text file. Not required if --country defined.')
    parser.add_argument('-c', '--country', dest='country', type=str, default='',
            help="Country of building footprints required. Not required if --aoi defined.")

    return parser.parse_args()


def main():
    # this is the name of the geography you want to retrieve. update to meet your needs
    # Geometry copied from https://geojson.io
    inps = cmdLineParse()


    # aoi_geom = {
    #     "coordinates": [
    #         [
    #             [-122.16484503187519, 47.69090474454916],
    #             [-122.16484503187519, 47.6217555345674],
    #             [-122.06529607517405, 47.6217555345674],
    #             [-122.06529607517405, 47.69090474454916],
    #             [-122.16484503187519, 47.69090474454916],
    #         ]
    #     ],
    #     "type": "Polygon",
    # }

    df = pd.read_csv("https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv")


    tmp_fns = []
    tmpdir = './tmp'

    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)

    os.makedirs(tmpdir)

    # get the gdf from the query
    if inps.country:
        location = inps.country
        output_fn = f"{location}_buildings.geojson"
        links = df[df.Location == location]
        ind=1
        length=len(links.index)
        for _, row in links.iterrows():
            df = pd.read_json(row.Url, lines=True)
            df['geometry'] = df['geometry'].apply(shapely.geometry.shape)
            gdf = gpd.GeoDataFrame(df, crs=4326)
            fn = os.path.join(tmpdir, f"{row.QuadKey}.geojson")
            # create the geojson files from the gdf
            print(f'Downloading {ind}/{length} for {row.Location} to tmp file: {fn}')
            gdf.to_file(fn, driver="GeoJSON")
            tmp_fns.append(fn)
            ind = ind+1


    if inps.aoi_file:
        with open(inps.aoi_file) as f:
            data = json.load(f)
        aoi_geom = data['features'][0]['geometry']  # Your first point
        quad_keys = set()
        output_fn = "{}_buildings.geojson".format(os.path.splitext(os.path.basename(inps.aoi_file))[0])
        aoi_shape = shapely.geometry.shape(aoi_geom)
        minx, miny, maxx, maxy = aoi_shape.bounds
        for tile in list(mercantile.tiles(minx, miny, maxx, maxy, zooms=9)):
            quad_keys.add(int(mercantile.quadkey(tile)))

        quad_keys = list(quad_keys)
        print(f"The input area spans {len(quad_keys)} tiles: {quad_keys}")

        # Download the GeoJSON files for each tile that intersects the input geometry
        for quad_key in tqdm(quad_keys):
            rows = df[df["QuadKey"] == quad_key]
            if rows.shape[0] > 0:
                if rows.shape[0] > 1:
                    print(f"Multiple rows found for QuadKey: {quad_key}")
                for index, row in rows.iterrows():
                    url = row["Url"]
                    df2 = pd.read_json(url, lines=True)
                    df2["geometry"] = df2["geometry"].apply(shapely.geometry.shape)
                    gdf = gpd.GeoDataFrame(df2, crs=4326)
                    fn = os.path.join(tmpdir, f"{quad_key}_{index}.geojson")
                    tmp_fns.append(fn)
                    # create the geojson files from the gdf
                    if not os.path.exists(fn):
                        gdf.to_file(fn, driver="GeoJSON")

            else:
                raise ValueError(f"QuadKey not found in dataset: {quad_key}")


    # merge the geojson files
    idx = 0
    combined_rows = []
    for fn in tmp_fns:
        with fiona.open(fn, "r") as f:
            for row in tqdm(f):
                row = dict(row)
                shape = shapely.geometry.shape(row["geometry"])
                add_row = True
                if inps.aoi_file:
                    add_row = aoi_shape.contains(shape)
                if add_row:
                    if "id" in row:
                        del row["id"]
                    row["properties"] = {"id": idx}
                    idx += 1
                    combined_rows.append(row)

    schema = {"geometry": "Polygon", "properties": {"id": "int"}}

    with fiona.open(output_fn, "w", driver="GeoJSON", crs="EPSG:4326", schema=schema) as f:
        f.writerecords(combined_rows)


if __name__ == "__main__":
    main()