from neo4j.v1 import GraphDatabase
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
from bson import SON
from pymongo import MongoClient, GEOSPHERE, ASCENDING
from datetime import datetime
import pandas as pd
import folium
from folium import plugins

#-----------------------------------------------------------------MONGODB FUNCTIONS---------------------------------------------------------------
def get_session_mon(db_name):
    """
    :param db_name: Nombre de la base de datos de los incidentes y los distritos
    :return: Objeto de tipo DataBase con una conexión a la base de datos local
    """
    client = MongoClient()
    db = client[db_name]
    return db


def create_indexes_mon(db):
    """
    GEOSPHERE: para procesar coordinadas esféricas
    :param db: Añade un índice sobre los campos que contienen información geoespacial
    """
    db.incidents.create_index([("Location", GEOSPHERE)])
    db.neighbours.create_index([("the_geom", GEOSPHERE)])
    db.incidents.create_indexes(["Date", ASCENDING])


def first_query_mon(db):
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


def second_query_mon(db):
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


def third_query_mon(db):
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


def since_february_mon(db):
    """
    :param db: referencia a la sesión de la bd
    :return: Devuelve los incidentes que han ocurrido desde febrero de 2018
    """
    fecha = datetime(2018, 2, 1)
    incidents = db.incidents.find({"Date": {"$gt": fecha}})
    df = pd.DataFrame(list(incidents))
    return df


def date_querry_mon(op, sdate, edate):
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


def generic_date_search_mon(db, op, sdate, *args, **kwargs):
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
        query = date_querry_mon(op, sdate, edate)
    else:
        query = date_querry_mon(op, sdate, None)
    query_results = db.incidents.find(query)
    df = pd.DataFrame(list(query_results))
    return df


def draw_map_mon(ds):
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


def draw_heatmap_mon(df):
    """
    Esta función recibe un conjunto de incidentes y los dibuja en un mapa de calor que
    se guarda en un fichero html
    :param ds: dataset de incidentes en formato de DataFrame
    """
    heat_map = folium.Map(location=[37.7617007179518, -122.42158168136999], zoom_start=11, tiles='Stamen Terrain')
    heat_map.add_child(plugins.HeatMap([[row["Y"], row["X"]] for name, row in df.iterrows()]))
    heat_map.save('heat_map_incidets.html')
    return heat_map
#-----------------------------------------------------------------MONGODB FUNCTIONS---------------------------------------------------------------

#-----------------------------------------------------------------CASSANDRA FUNCTIONS-------------------------------------------------------------
# SOME VARIABLES NEEDED FOR CONNECT TO THE DATABASE AND RETRIEVE DATA
cluster_cas = Cluster(['127.0.0.1'])
session_cas = cluster_cas.connect()
session_cas.set_keyspace('incidencias')

#ver todas las categorias
def get_categories_cas():
	rows_categories = session_cas.execute('SELECT category FROM categorias')
	categories = []
	for category in rows_categories:
   		categories.append(category.category)
	return categories

"""
categories_cas = get_categories_cas()
for category in categories_cas:
    print (category)
"""

#ver todas los distritos
def get_districts_cas():
	rows_districts = session_cas.execute('SELECT pddistrict FROM distritos')
	districts = []
	for district in rows_districts:
   		districts.append(district.pddistrict)
	return districts

"""
districts_cas = get_districts_cas()
for district in districts_cas:
    print(district)
"""

#cantidad distritos
def get_num_districts_cas():
	num_districts = session_cas.execute('SELECT count(*) FROM distritos')
	return num_districts[0].count

"""
num_districts_cas = get_num_districts_cas()
print(num_districts_cas)
"""

#Listar incidencias según el distrito'
def get_incidents_by_district_cas(district):
	rows_incidents = session_cas.execute('SELECT * FROM incidencias.incidenciasbyzona WHERE pddistrict=%s',[district])
	return rows_incidents

"""
rows_incidents_cas = get_incidents_by_district_cas('NORTHERN')
for incident in rows_incidents_cas:
    print (incident.pdid)
"""

#Número incidencias según el distrito'
def get_num_incidents_by_district_cas(district):
	num_incidents = session_cas.execute('SELECT COUNT(*) FROM incidencias.incidenciasbyzona WHERE pddistrict=%s',[district])
	return num_incidents[0].count

"""
num_incidents_cas = get_num_incidents_by_district_cas('NORTHERN')
print(num_incidents_cas)
"""

#Listar incidencias según el distrito y la categoria
def get_incidents_by_category_district_cas(category,district):
	rows_incidents = session_cas.execute('SELECT * FROM incidencias.incidenciasbycategoriazona WHERE Category=%s AND pddistrict=%s',[category,district])
	return rows_incidents
"""
rows_incidents_cas = get_incidents_by_category_district_cas('SUICIDE','BAYVIEW')
for incident in rows_incidents_cas:
    print (incident.pdid)
"""

#Número de incidencias según el distrito y la categoria
def get_num_incidents_by_category_district_cas(category,district):
	num_incidents = session_cas.execute('SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazona WHERE Category=%s AND pddistrict=%s',["SUICIDE","BAYVIEW"])
	return num_incidents[0].count

"""
num_incidents_cas = get_num_incidents_by_category_district_cas('SUICIDE','BAYVIEW')
print(num_incidents_cas)
"""

#Listar incidencias según el distrito, la categoria, fecha inicio y fin
def get_incidents_by_category_district_betweendate_cas(category,district,date_start,date_end):
	rows_incidents = session_cas.execute('SELECT * FROM incidencias.incidenciasbycategoriazonafecha WHERE Category=%s AND pddistrict=%s AND date >= %s AND date <= %s',["SUICIDE","BAYVIEW","2017-06-01","2018-08-01"])
	return rows_incidents
"""
rows_incidents_cas = get_incidents_by_category_district_betweendate_cas('SUICIDE','BAYVIEW','2017-06-01','2018-08-01')
for incident in rows_incidents_cas:
    print (incident.pdid)
"""

#Número de incidencias según el distrito, la categoria, fecha inicio y fin
def get_num_incidents_by_category_district_betweendate_cas(category,district,date_start,date_end):
	num_incidents = session_cas.execute('SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazonafecha WHERE Category=%s AND pddistrict=%s AND date >= %s AND date <= %s',["SUICIDE","BAYVIEW","2017-06-01","2018-08-01"])
	return num_incidents[0].count
"""
num_incidents_cas = get_num_incidents_by_category_district_betweendate_cas('SUICIDE','BAYVIEW','2017-06-01','2018-08-01')
print(num_incidents_cas)
"""
#-----------------------------------------------------------------CASSANDRA FUNCTIONS-------------------------------------------------------------

#-----------------------------------------------------------------NEO4J FUNCTIONS-----------------------------------------------------------------
# SOME VARIABLES NEEDED FOR CONNECT TO THE DATABASE AND RETRIEVE DATA
uri_neo = "bolt://localhost:7687"  # DATABASE URI
driver_neo = GraphDatabase.driver(uri_neo, auth=("neo4j", "admin"))  # DRIVER NEO4J
session_neo = driver_neo.session()  # SESSION
tx_neo = session_neo.begin_transaction()  # TRANSACTION

# WE DEFINE SOME METHODS
def get_districts_neo():  # DISTRICTS LIST
    list_dis = []
    for record in tx_neo.run("MATCH (dis:District) RETURN dis.district"):
        aux = record["dis.district"]
        # print(aux)
        list_dis.append(aux)
    return list_dis


def get_count_districts_neo():  # NUMBER OF DISTRICTS
    record = tx_neo.run("MATCH (dis:District) RETURN count(dis)")
    count = record.single()[0]
    return count


def get_count_incidents_district_neo(district):  # NUMBER OF INCIDENTS IN A DISTRICT
    record = tx_neo.run("MATCH (dis:District {district: $district}) RETURN size(()-[:WHERE]->(dis))", district=district)
    count = record.single()[0]
    return count


def get_categories_neo():  # CATEGORIES LIST
    list_cat = []
    for record in tx_neo.run("MATCH (cat:Category) RETURN cat.category"):
        aux = record["cat.category"]
        # print(aux)
        list_cat.append(aux)
    return list_cat


def get_count_categories_neo():  # NUMBER OF CATEGORIES
    record = tx_neo.run("MATCH (cat:Category) RETURN count(cat)")
    count = record.single()[0]
    return count


def get_count_incidents_category_neo(category):  # NUMBER OF INCIDENTS OF A CATEGORY
    record = tx_neo.run("MATCH (cat:Category {category: $category}) RETURN size(()-[:WHAT]->(cat))", category=category)
    count = record.single()[0]
    return count


def get_count_incidents_district_category_neo(district, category):  # NUMBER OF INCIDENTS IN A DISTRICT OF A CATEGORY
    record = tx_neo.run(
        "MATCH (cat:Category {category: $category}), (dis:District {district: $district}) RETURN size((cat)<-["
        ":WHAT]-()-[:WHERE]->(dis))",
        district=district, category=category)
    count = record.single()[0]
    return count
#-----------------------------------------------------------------NEO4J---------------------------------------------------------------------------



#-----------------------------------------------------------------MAIN----------------------------------------------------------------------------
if __name__ == "__main__":
	#----------MONGODB------------
    sfdb_mon = get_session_mon("san_francisco_incidents")  # Obtenemos la bd con su nombre
    create_indexes_mon(sfdb_mon)  # Nos aseguramos de que existan índices geo-espaciales
    fq_mon = first_query_mon(sfdb_mon)  # Consulta geoespacial con operador $near
    sq_mon = second_query_mon(sfdb_mon)  # Consulta geoespacial con el operador $geoIntersects
    tq_mon = third_query_mon(sfdb_mon)  # Consulta geoespacial
    # Fechas para filtro de fechas
    fecha1_mon = datetime(2017, 12, 1)
    fecha2_mon = datetime(2017, 12, 31)
    # Buscaremos los incidentes entre dos fechas, B=Between (ver doc de la función)
    date_results_mon = generic_date_search_mon(sfdb_mon, 'B', fecha1_mon, edate=fecha2_mon)
    # Buscamos todos los incidentes de febrero
    feb_mon = since_february_mon(sfdb_mon)  # Consulta específica a una fecha
    # Generamos un mapa con los miles primeros incidentes
    m_mon = draw_map_mon(feb_mon.iloc[:1000])
    hm_mon = draw_heatmap_mon(date_results_mon.iloc[:1000])

    #----------CASSANDRA----------
    print("CASSANDRA --> Number of districts: ", get_num_districts_cas())
	print("CASSANDRA --> Number of incidents in NORTHERN: ", get_num_incidents_by_district_cas('NORTHERN')) 
	print("CASSANDRA --> Number of cases of SUICIDE in BAYVIEW between 2017-06-01 and 2018-08-01: ", get_num_incidents_by_category_district_betweendate_cas('SUICIDE','BAYVIEW','2017-06-01','2018-08-01'))

	#Plot
	districts_cas = get_districts_cas()
	list_inc_cas = []
	for dis in districts_cas: # RETRIVE THE COUNT OF INCIDENTS FOR EACH DISTRICT
	    list_inc_cas.append(get_num_incidents_by_district(dis))

	fig_cas = plt.figure()
	plot_cas = fig_cas.add_subplot(111)

	xx_cas = range(1, len(list_inc_cas)+1)

	# BAR CHART
	plot_cas.bar(xx_cas, list_inc_cas, width=0.5)
	plot_cas.set_xticks(xx_cas)
	plot_cas.set_xticklabels(districts_cas)
	plot_cas.set_title('Incidents by district')
	plot_cas.set_xlabel('Districts')
	plot_cas.set_ylabel('Number of incidents')
	plot_cas.tick_params(axis='both', which='major', labelsize=6)

	plt.show() # SHOW THE BAR CHART
	fig_cas.savefig('incidents_by_district_cas.png') # SAVE THE CHART

	#----------NEO4J--------------
	districts_neo = get_districts_neo()
	n_districts_neo = get_count_districts_neo()
	print("NEO --> Number of districts: " + str(n_districts_neo))
	# print("NEO --> " + get_count_incidents_district_neo("TENDERLOIN"))

	categories_neo = get_categories_neo()
	n_categories_neo = get_count_categories_neo()
	print("NEO --> Number of categories: " + str(n_categories_neo))
	# print("NEO --> " + get_count_incidents_category_neo("DRUNKENNESS"))

	# print("NEO --> " + get_count_incidents_district_category_neo("BAYVIEW", "DRUG/NARCOTIC"))

	# LET'S PLOT A CHART!!
	list_inc_neo = []
	for dis in districts_neo: # RETRIVE THE COUNT OF INCIDENTS FOR EACH DISTRICT
	    list_inc_neo.append(get_count_incidents_district_neo(dis))

	fig_neo = plt.figure()
	plot_neo = fig_neo.add_subplot(111)

	xx_neo = range(1, len(list_inc_neo)+1)

	# BAR CHART
	plot.bar(xx_neo, list_inc_neo, width=0.5)
	plot.set_xticks(xx_neo)
	plot.set_xticklabels(districts_neo)
	plot.set_title('Incidents by district')
	plot.set_xlabel('Districts')
	plot.set_ylabel('Number of incidents')
	plot.tick_params(axis='both', which='major', labelsize=6)

	plt.show() # SHOW THE BAR CHART
	fig_neo.savefig('incidents_by_district_neo.png') # SAVE THE CHART

	# CLOSE DATABASE SESSION
	session_neo.close()

	#----------MACHINE LEARNING---
	# To be continued...

