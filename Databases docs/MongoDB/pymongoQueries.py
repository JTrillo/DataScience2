# Author: Group 6

from bson import SON
from pymongo import MongoClient, GEOSPHERE, ASCENDING
from datetime import datetime
import pandas as pd
import folium
from folium import plugins


def get_session(db_name):
    """
    :param db_name: Nombre de la base de datos de los incidentes y los distritos
    :return: Objeto de tipo DataBase con una conexión a la base de datos local
    """
    client = MongoClient()
    db = client[db_name]
    return db


def create_indexes(db):
    """
    GEOSPHERE: para procesar coordinadas esféricas
    :param db: Añade un índice sobre los campos que contienen información geoespacial
    """
    db.incidents.create_index([("Location", GEOSPHERE)])
    db.neighbours.create_index([("the_geom", GEOSPHERE)])
    db.incidents.create_indexes(["Date", ASCENDING])


def first_query(db):
    """
    :param db: referencia a la sesión de la bd
    :return: Esta función obtiene los incidentes que están a una distancia máximo de 1000 metros
    desde un punto representado por coordinadas geográficas en formato geojson
    """
    # Montamos la query
    query_incidents = {
        "Location": {
            "$near": {
                "$geometry": SON([
                    ("type", "Point"),
                    ("coordinates", [-122.42158168136999, 37.7617007179518])
                ]),
                "$maxDistance": 1000
            }
        }
    }
    # Ejecutamos la querry sobre la colección de incidencias
    query_results = db.incidents.find(query_incidents)
    df = pd.DataFrame(list(query_results))
    return df


def second_query(db):
    """
    :param db: referencia a la sesión de la bd
    :return: devuelve el distrito que contiene las coordinadas usadas en el
    operado de intersección
    """
    # Montamos la query
    query_distrito = {"the_geom":
        {"$geoIntersects":
            {"$geometry": SON([
                ("type", "Point"),
                ("coordinates", [-122.42158168136999, 37.7617007179518])])
            }
        }
    }
    # Ejecutamos la querry sobre la colección de incidencias
    query_results = db.neighbours.find_one(query_distrito)
    return query_results


def third_query(db):
    """
    :param db: referencia a la sesión de la bd
    :return: Devuelve el todos los incidentes, en dataframe, de un distrito, usando
    operadores geo-espaciales, la consulta se realiza en fases sobre las dos
    colecciones, primero encontramos el distrito, y luego buscamos todos los
    incidentes que tienen las coordinadas dentro del polígono
    """
    # Empezamos con el distrito
    query_distrito = {"the_geom": {"$geoIntersects": {
        "$geometry": SON([("type", "Point"), ("coordinates", [-122.42158168136999, 37.7617007179518])])}}}
    distrito = db.neighbours.find_one(query_distrito)
    # Ahora encontramos los incidentes
    query_incidents = {"Location": {"$geoWithin": {"$geometry": distrito['the_geom']}}}
    query_results = db.incidents.find(query_incidents)
    df = pd.DataFrame(list(query_results))
    return df


def since_february(db):
    """
    :param db: referencia a la sesión de la bd
    :return: Devuelve los incidentes que han ocurrido desde febrero de 2018
    """
    fecha = datetime(2018, 2, 1)
    incidents = db.incidents.find({"Date": {"$gt": fecha}})
    df = pd.DataFrame(list(incidents))
    return df


def date_querry(op, sdate, edate):
    """
    :param opr: operador B: between dates, L: less than, G: greater than
    :return: devuelve el filtro de la consulta según lo que se recibe por parámetro
    en op
    """
    switcher = {
        'B': {"Date": {"$gte": sdate, "$lte": edate}},  # Between
        'GE': {"Date": {"$gte": sdate}},  # Greater than or equal
        'LE': {"Date": {"$lte": sdate}},  # less than or equal
    }
    return switcher.get(op)


def generic_date_search(db, op, sdate, *args, **kwargs):
    """
    :param db: referencia a la sesión de bd
    :param op: operador para seleccionar
    :param sdate: primera fecha, mandaterio
    :param args:
    :param kwargs: Contiene argumento opcional de la seguna fecha
    :return: incidentes que cumplen con el filtro sobre fechas
    """
    edate = kwargs.get('edate', None)
    if not edate is None:
        query = date_querry(op, sdate, edate)
    else:
        query = date_querry(op, sdate, None)
    query_results = db.incidents.find(query)
    df = pd.DataFrame(list(query_results))
    return df


def draw_map(ds):
    """
    Esta función recibe un conjunto de incidentes y los dibuja en un mapa usando el paquete folium,
    el mapa se guarda en un fichero.
    :param ds: dataset de incidentes en formato de DataFrame
    """
    incid_map = folium.Map(location=[37.7617007179518, -122.42158168136999], zoom_start=11, tiles='Stamen Terrain')
    marker_cluster = plugins.MarkerCluster().add_to(incid_map)
    for name, row in ds.iterrows():
        folium.Marker([row["Y"], row["X"]], popup=row["Descript"]).add_to(marker_cluster)
    incid_map.save('incidents.html')
    return incid_map


def draw_heatmap(df):
    """
    Esta función recibe un conjunto de incidentes y los dibuja en un mapa de calor que
    se guarda en un fichero html
    :param ds: dataset de incidentes en formato de DataFrame
    """
    heat_map = folium.Map(location=[37.7617007179518, -122.42158168136999], zoom_start=11, tiles='Stamen Terrain')
    heat_map.add_child(plugins.HeatMap([[row["Y"], row["X"]] for name, row in df.iterrows()]))
    heat_map.save('heat_map_incidets.html')
    return heat_map


if __name__ == "__main__":
    sfdb = get_session("san_francisco_incidents")  # Obtenemos la bd con su nombre
    create_indexes(sfdb)  # Nos aseguramos de que existan indíces geo-espaciales
    fq = first_query(sfdb)  # Consulta geoespacial con operador $near
    sq = second_query(sfdb)  # Consulta geoespacial con el operador $geoIntersects
    tq = third_query(sfdb)  # Consulta geoespacial
    # Fechas para filtro de fechas
    fecha1 = datetime(2017, 12, 1)
    fecha2 = datetime(2017, 12, 31)
    # Buscaremos los incidentes entre dos fechas, B=Between (ver doc de la función)
    date_results = generic_date_search(sfdb, 'B', fecha1, edate=fecha2)
    # Buscamos todos los incidentes de febrero
    feb = since_february(sfdb)  # Consulta específica a una fecha
    # Generamos un mapa con los miles primeros incidentes
    m = draw_map(feb.iloc[:1000])
    hm = draw_heatmap(date_results.iloc[:1000])

