#!/usr/bin/env python

from cassandra.cluster import Cluster
import matplotlib.pyplot as plt

#cluster = Cluster()
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('incidencias')



#ver todas las categorias
def get_categories():
	rows_categories = session.execute('SELECT category FROM categorias')
	categories = []
	for category in rows_categories:
   		categories.append(category.category)
	return categories

"""
categories = get_categories()
for category in categories:
    print (category)
"""

#ver todas los distritos
def get_districts():
	rows_districts = session.execute('SELECT pddistrict FROM distritos')
	districts = []
	for district in rows_districts:
   		districts.append(district.pddistrict)
	return districts

"""
districts = get_districts()
for district in districts:
    print(district)
"""


#cantidad distritos
def get_num_districts():
	num_districts = session.execute('SELECT count(*) FROM distritos')
	return num_districts[0].count

"""
num_districts = get_num_districts()
print(num_districts)
"""


#Listar incidencias según el distrito'
def get_incidents_by_district(district):
	rows_incidents = session.execute('SELECT * FROM incidencias.incidenciasbyzona WHERE pddistrict=%s',[district])
	return rows_incidents

"""
rows_incidents = get_incidentsbydistrict('NORTHERN')
for incident in rows_incidents:
    print (incident.pdid)
"""

#Número incidencias según el distrito'
def get_num_incidents_by_district(district):
	num_incidents = session.execute('SELECT COUNT(*) FROM incidencias.incidenciasbyzona WHERE pddistrict=%s',[district])
	return num_incidents[0].count

"""
num_incidents = get_num_incidents_by_district('NORTHERN')
print(num_incidents)
"""



#Listar incidencias según el distrito y la categoria
def get_incidents_by_category_district(category,district):
	rows_incidents = session.execute('SELECT * FROM incidencias.incidenciasbycategoriazona WHERE Category=%s AND pddistrict=%s',[category,district])
	return rows_incidents
"""
rows_incidents = get_incidents_by_category_district('SUICIDE','BAYVIEW')
for incident in rows_incidents:
    print (incident.pdid)
"""



#Número de incidencias según el distrito y la categoria
def get_num_incidents_by_category_district(category,district):
	num_incidents = session.execute('SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazona WHERE Category=%s AND pddistrict=%s',["SUICIDE","BAYVIEW"])
	return num_incidents[0].count

"""
num_incidents = get_num_incidents_by_category_district('SUICIDE','BAYVIEW')
print(num_incidents)
"""

#Listar incidencias según el distrito, la categoria, fecha inicio y fin
def get_incidents_by_category_district_betweendate(category,district,date_start,date_end):
	rows_incidents = session.execute('SELECT * FROM incidencias.incidenciasbycategoriazonafecha WHERE Category=%s AND pddistrict=%s AND date >= %s AND date <= %s',["SUICIDE","BAYVIEW","2017-06-01","2018-08-01"])
	return rows_incidents
"""
rows_incidents = get_incidents_by_category_district_betweendate('SUICIDE','BAYVIEW','2017-06-01','2018-08-01')
for incident in rows_incidents:
    print (incident.pdid)
"""

#Número de incidencias según el distrito, la categoria, fecha inicio y fin
def get_num_incidents_by_category_district_betweendate(category,district,date_start,date_end):
	num_incidents = session.execute('SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazonafecha WHERE Category=%s AND pddistrict=%s AND date >= %s AND date <= %s',["SUICIDE","BAYVIEW","2017-06-01","2018-08-01"])
	return num_incidents[0].count
"""
num_incidents = get_num_incidents_by_category_district_betweendate('SUICIDE','BAYVIEW','2017-06-01','2018-08-01')
print(num_incidents)
"""



print("Number of districts: ", get_num_districts())
print("Number of incidents in NORTHERN: ", get_num_incidents_by_district('NORTHERN')) 
print("Number of cases of SUICIDE in BAYVIEW between 2017-06-01 and 2018-08-01: ", get_num_incidents_by_category_district_betweendate('SUICIDE','BAYVIEW','2017-06-01','2018-08-01'))


#Plot
districts = get_districts()
list_inc = []
for dis in districts: # RETRIVE THE COUNT OF INCIDENTS FOR EACH DISTRICT
    list_inc.append(get_num_incidents_by_district(dis))

fig = plt.figure()
plot = fig.add_subplot(111)

xx = range(1, len(list_inc)+1)

# BAR CHART
plot.bar(xx, list_inc, width=0.5)
plot.set_xticks(xx)
plot.set_xticklabels(districts)
plot.set_title('Incidents by district')
plot.set_xlabel('Districts')
plot.set_ylabel('Number of incidents')
plot.tick_params(axis='both', which='major', labelsize=6)

plt.show() # SHOW THE BAR CHART
fig.savefig('incidents_by_district.png') # SAVE THE CHART
