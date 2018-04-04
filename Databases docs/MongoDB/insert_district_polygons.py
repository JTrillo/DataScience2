#!/usr/bin/env python
# Author: Group 6
import pandas as pd
import ijson
from shapely.geometry import mapping
from shapely.wkt import loads
from pymongo import MongoClient


# Convierte datos geoespaciales en formato wkt a geojson, ya que mongodb no admite wkt
def wkt_to_geojson(wkt):
    wkt_multipol = loads(wkt)
    gj_multipol = mapping(wkt_multipol)
    return gj_multipol


# Extrae los nombres de columnas del json, y extrae los datos
def import_content(path):
    # Extract the names of the columns from the json
    with open(file_path, 'r') as f:
        objects = ijson.items(f, 'meta.view.columns.item')
        columns = list(objects)
    column_names = [col["fieldName"] for col in columns]
    # Extract the data from the json
    final_columns = ['the_geom', 'nhood']
    data = []
    with open(path, 'r') as f:
        objects = ijson.items(f, 'data.item')
        for row in objects:
            selected_row = []
            for item in final_columns:
                selected_row.append(row[column_names.index(item)])
            data.append(selected_row)
    # We convert to pandas dataframe to ease the proces
    df = pd.DataFrame(data, columns=final_columns)
    df["the_geom"] = df["the_geom"].apply(wkt_to_geojson)
    return df


# Inserta en la base de datos todas las líneas del dataframe ne forma de documentos json
def mongo_insert(df):
    client = MongoClient()
    db = client['san_francisco_incidents1']
    neighbours = db.neighbours
    neighbours.insert_many(df.to_dict("records"))


if __name__ == "__main__":
    file_path = "/Users/yasos/Downloads/rows.json"
    df = import_content(file_path)
    mongo_insert(df)