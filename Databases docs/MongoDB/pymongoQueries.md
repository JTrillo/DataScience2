# Consultas de MongoDB
primero tenemos que usar la base de datos que nos interesa usando este comando:
```
use san_francisco_incidents
```
Antes de realizar las consultas vamos a crear algunos indíces que nos ayudarán a realizar consultas más optimas. Crearemos un indíce sobre los campos Category, DayOfWeek y PdDistrict. El valor 1 en la función createIndex denota que queremos que sea incremental el orden. Crearemos además un indíce espacial sobre la clave Location para poder realizar consultas geoespaciales, y otro sobre el campo the_geom de la colección de distritos. Los índices geo-espaciales se definen pasando por parámetro el valor "2dsphere" para la clave que representa el campo sobre el que se va a crear el índice.
```
db.incidents.createIndex( { category: 1 } )
db.incidents.createIndex( { DayOfWeek: 1 } )
db.incidents.createIndex( { PdDistrict: 1 } )
db.incidents.createIndex( { location : "2dsphere" } )
db.neighbours.createIndex( { the_geom : "2dsphere" } )
```
### Obtener todas las incidencias
```
db.incidents.find( {} )
```
### Obtener el total de incidencias
```
db.incidents.count()
```
### Obtener las incidencias del distrito "MISSION" y obtener el total
```
db.incidents.find({PdDistrict: "MISSION"})
db.incidents.count({PdDistrict: "MISSION"})
```
### Obtener las incidencias con resolución "ARREST, BOOKED" y han ocurrido en marzo 2015
```
db.incidents.find({
    Resolution: 'ARREST, BOOKED',
    Date: {
        $gt: ISODate('2015-03-01T00:00:00.000Z'),
        $lt: ISODate('2015-03-31T00:00:00.000Z')
        }
    }
)
```
### Obtener el total de incidencias de cada categoría usando MapReduce Framework
```
db.incidents.mapReduce(
    function() { emit(this._id, 1); },
    function(key, values) { var total = 0;
                  for (var i = 0; i < values.length; i++) {
                    total += values[i];
                  }
                  return total; },
    { out: "MapReduceQuery" }
)
```
### Obtener el total de incidencias de cada categoría usando Aggregate Framework
```
db.incidents.aggregate(
    [{
        $group:{
            _id : "$Category",
            total:{$sum: 1}
        }
    }]
)
```
### Obtener el total de incidencias en cada día de la semana usando Aggregate Framework
```
db.incidents.aggregate(
    [{
        $group:{
            _id : "$DayOfWeek",
            total:{$sum: 1}
        }
    }]
)
```
### Acceso a la base de datos desde python, y import de todos los paquetes necesarios
En esta entrega no se van a detallar los pasos de volcado de datos desde python en mongodb, ya que se ha realizado en la entrega anterior.
```
from pymongo import MongoClient, GEOSPHERE
# La base de datos de incidentes se llama san_francisco_incidents
client = MongoClient()
db = client()['san_francisco_incidents']
```
Para poder realizar las consultas siguientes crearemos varios indices sobre varios campos en la colección de incidencias y de distritos.
```
db.incidents.create_index([("Location", GEOSPHERE)]) #GEOSPHERE: para procesar coordinadas esféricas
db.neighbours.create_index([("the_geom", GEOSPHERE)])
```
### Obtener las incidencias que han ocurrido a una distancia máxima de 1000 metros alrededor de las coordinadas (37.7813411,-122.4112952) 
```
query = {"Location" : {"$near": {"$geometry" : SON([("type", "Point"), ("coordinates", [-122.4, 37.7])]), "$maxDistance": 1000}}}`
db.incidents.find(query)
```
### Obtener el distrito de una incidencia con consultas geo-espatiales
La siguiente consulta responde al siguiente requisito: dadas las coordinadas de un incidente, por ejemplo (-122.4112952, 37.7813411), obtener el poligono de la collección de "neighbours" que forma al distrito en que ha ocurrido el incidente.
Para esta consulta, se usa el operador $geoIntersects que recibe como parámetro un punto en formato geo-json y realiza una búsqueda geo-espacial sobre el campo "the_geom" de la colleción "neighbours".
```
query = {"the_geom" : {"$geoIntersects": {"$geometry" : SON([("type", "Point"), ("coordinates", [-122.42158168136999, 37.7617007179518])])}}}
distrito_resultado = db.neighbours.find_one(query)
```
### Obtener todas las incidencias que estén en un distrito usando consultas geo-espaciales
La siguiente consulta, obtiene primero un distrito representado por un multipoligono, usando las coordinadas de un punto. Luego usando ese multipoligono realiza una búsqueda geo-espacial en la que usa el operador $geoWithin, que como su nombre lo sugiere, devuelve los documentos de incidencias que sus coordinadas estén situadas dentro de ese poligono.
El uso de objeto SON es necesario para poder definir geometrías con más de un atributo, por ejemplo el punto tiene la clave type y la clave coordinates.
```
# Montamos la query para encontrar el distrito en el que está el punto
query_distrito = {"the_geom" : {"$geoIntersects": {"$geometry": SON([("type", "Point"), ("coordinates", [-122.42158168136999, 37.7617007179518])])}}}
# Realizamos la consulta a la colección de distritos en mongodb
distrito = db.neighbours.find_one(query_distrito)
# Montamos la query para encontrar todos los incidents que están en el distrito anterior
query_incidents = {"Location": {"$geoWithin": {"$geometry": distrito['the_geom']}}}
# Realizamos la consulta a la colección de incidentes en mongodb
incidentes = db.incidents.find(query_incidents)
```
