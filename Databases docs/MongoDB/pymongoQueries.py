# Author: Group 6

from bson import SON
from pymongo import MongoClient, GEOSPHERE


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


def first_query(db):
    """
    :param db: referencia a la sesión de la bd
    :return: Esta función obtiene los incidentes que están a una distancia máximo de 1km
    desde un punto representado por coordinadas geográficas en formato geojson
    """
    # Montamos la query
    query_incidents = {"Location":
        {"$geoIntersects":
            {"$geometry": SON([
                ("type", "Point"),
                ("coordinates", [-122.42158168136999, 37.7617007179518])
            ])}
        }
    }
    # Ejecutamos la querry sobre la colección de incidencias
    query_results = db.incidents.find(query_incidents)
    return query_results


def second_query(db):
    """
    :param db: referencia a la sesión de la bd
    :return: devuelve el distrito que contiene las coordinadas usadas en el
    operado de intersección
    """
    # Montamos la query
    query_distrito = {"Location":
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
    :return: Devuelve el todos los incidentes que de un distrito, usando
    operadores geo-espaciales, la consulta se realiza en fases sobre las dos
    colecciones, primero encontramos el distrito, y luego buscamos todos los
    incidentes que tienen las coordinadas dentro del polígono
    """
    query_distrito = {"the_geom": {"$geoIntersects": {
        "$geometry": SON([("type", "Point"), ("coordinates", [-122.42158168136999, 37.7617007179518])])}}}
    distrito = db.neighbours.find_one(query_distrito)
    query_incidents = {"Location": {"$geoWithin": {"$geometry": distrito['the_geom']}}}
    query_results = db.incidents.find(query_incidents)
    return query_results


if __name__ == "__main__":
    sfdb = get_session("san_francisco_incidents")  # Obtenemos la bd con su nombre
    create_indexes(sfdb)

    fq = first_query(sfdb)
    sq = second_query(sfdb)
    tq = third_query(sfdb)
