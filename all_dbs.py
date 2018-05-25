from neo4j.v1 import GraphDatabase
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt

#-----------------------------------------------------------------CASSANDRA-----------------------------------------------------------------------
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

#-----------------------------------------------------------------CASSANDRA-----------------------------------------------------------------------

#-----------------------------------------------------------------NEO4J---------------------------------------------------------------------------
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


# TESTING METHODS

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
#-----------------------------------------------------------------NEO4J---------------------------------------------------------------------------