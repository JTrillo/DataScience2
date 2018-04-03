# Consultas de MongoDB
primero tenemos que usar la base de datos que nos interesa usando este comando:
```
use san_francisco_incidents
```
Antes de realizar las consultas vamos a crear algunos indíces que nos ayudarán a realizar consultas más optimas. Crearemos un indíce sobre los campos Category, DayOfWeek y PdDistrict. Crearemos además un indíce espacial sobre la columan Location para poder realizar consultas geoespaciales.
```
db.incidents.createIndex( { category: 1 } )
db.incidents.createIndex( { DayOfWeek: 1 } )
db.incidents.createIndex( { PdDistrict: 1 } )
db.incidents.createIndex( { location : "2dsphere" } )
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
    }], 
    {allowDiskUse: true})
```
### Obtener el total de incidencias en cada día de la semana usando Aggregate Framework
```
db.incidents.aggregate(
    [{
        $group:{
            _id : "$DayOfWeek",
            total:{$sum: 1}
        }
    }], 
    {allowDiskUse: true})
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
