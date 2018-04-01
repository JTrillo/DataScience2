#!/usr/bin/env python
# Author: Group 6
from pymongo import MongoClient
import pandas as pd
from datetime import datetime


# This function converts each argument to a float value, in case of Exception returns a 0
def parse_float(x):
    try:
        x = float(x)
    except Exception:
        x = 0
    return x


# This function receives a row of the dataframe and converts its date cell to a well formatted datetime object
# using the column Date and Time.
def parse_complete_date(s):
    date = datetime.strptime(s["Date"], "%m/%d/%Y")
    time = s["Time"].split(":")
    date_time = date.replace(hour=int(time[0]), minute=int(time[1]))
    return date_time


# This function receives a row from the dataframe
def parse_location(s):
    lat = s["Y"]
    log = s["X"]
    location_dict = {
        'coordinates': [log, lat],
        'type': 'point'
    }
    return location_dict


def import_content(file_path):
    client = MongoClient()
    db = client['san_francisco_incidents1']  # Creates the database for the San Francisco city incidents data
    incid = db.incidents        # Creates the collection of the documents of that will represent the incidents
    print("Reading csv file...")
    df = pd.read_csv(file_path)  # Reads the csv file into python's dataframe type (in my case I named it incid.csv)
    # We start parsing the dataframe columns
    df["X"] = df["X"].apply(parse_float)  # We make sure the fields X and Y are of type float
    df["Y"] = df["Y"].apply(parse_float)
    print("Parsing location...")
    df["Location"] = df.apply(parse_location, axis=1)  # Parse location into a well-formatted geojson object
    print("Parsing date...")
    df["Date"] = df.apply(parse_complete_date, axis=1)  # Axis=1 to interate over the rows applying parse_complete_date to each one
    print("Inserting data into monogodb. Might take some minutes, please be patient...")
    incid.insert_many(df.to_dict("records"))  # Insert many python objects into the collection incidents
    print("Done :D")


if __name__ == "__main__":
    file_path = "~/incidents.csv"
    import_content(file_path)
