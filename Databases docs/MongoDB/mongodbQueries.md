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
### Obtener las incidencias que han ocurrido a una distancia máxima de 1000 metros y una distancia mínima de 500 metros alrededor de las coordinadas (37.7813411,-122.4112952)
```
db.incidents.find({
    Location: {
        $near:{
            $geometry: { type: "point",  coordinates: [ -122.4112952, 37.7813411 ]},
            $minDistance: 500,
            $maxDistance: 1000
        }
    }
})
```
### Obtener el distrito de una incidencia con consultas geo-espatiales
La siguiente consulta responde al siguiente requisito: dadas las coordinadas de un incidente, por ejemplo (-122.4112952, 37.7813411), obtener el poligono de la collección de "neighbours" que forma al distrito en que ha ocurrido el incidente.
Para esta consulta, se usa el operador $geoIntersects que recibe como parámetro un punto en formato geo-json y realiza una búsqueda geo-espacial sobre el campo "the_geom" de la colleción "neighbours".
```
db.neighbours.findOne({
    the_geom: {
        $geoIntersects: {
            $geometry: {
                "type": "Point",
                "coordinates": [-122.42158168136999, 37.7617007179518]
            }
        }
    }
})
```
### Obtener todas las incidencias que estén en un distrito usando consultas geo-espaciales
La siguiente consulta, obtiene primero un distrito representado por un multipoligono (más información en https://tools.ietf.org/html/rfc7946#section-3.1.3) usando las coordinadas de un punto, que supongas son las de la incidencia de la consulta anterior. Luego usando ese multipoligono realiza una búsqueda geo-espacial en la que usa el operador $geoWithin, que como su nombre lo sugiere, devuelve los documentos de incidencias que sus coordinadas estén situadas dentro de ese poligono.
```
var neighborhood = db.neighbours.findOne({
    the_geom: { 
        $geoIntersects: {
            $geometry: {
                type: "Point",
                coordinates: [-122.42158168136999, 37.7617007179518]
            }
        }
    }
})
db.incidents.find({
    Location: {
        $geoWithin: { 
            $geometry: neighborhood.the_geom 
        }
    }
})
```

