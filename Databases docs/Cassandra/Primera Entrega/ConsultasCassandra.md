# Algunas consultas en Cassandra
## Ver todas las categorias de incidentes que existen
```sql
-- Creación tabla
CREATE TABLE incidencias.categorias( 
   Category text,
   PRIMARY KEY(Category)
);

-- Importar datos 
COPY incidencias.categorias (Category) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/categorias.csv' WITH HEADER = TRUE;

-- Número de categorias 
SELECT COUNT(*) FROM incidencias.categorias;

-- Listar todas las categorias
SELECT * FROM incidencias.categorias;
```

## Ver incidencias por zonas de la ciudad
```sql
-- Creación tabla
CREATE TABLE incidencias.incidenciasporzona( 
   Category text,
   Descript text,
   Date date,
   Time time,
   PdDistrict text,
   Resolution text,
   Location text,
   PdId text,
   PRIMARY KEY(PdDistrict,PdId)
)WITH CLUSTERING ORDER BY (PdId ASC);
-- Importar datos 
COPY incidencias.incidenciasporzona (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasporzona.csv' WITH HEADER = TRUE;

-- Listar incidencias donde el distrito sea 'NORTHERN'
SELECT * FROM incidencias.incidenciasbyzona WHERE pddistrict='NORTHERN';

-- Número de incidencias donde el distrito sea 'NORTHERN'
SELECT COUNT(*) FROM incidencias.incidenciasbyzona WHERE pddistrict='NORTHERN';
```

## Ver incidencias por categoria y zona
```sql
-- Creación tabla
CREATE TABLE incidencias.incidenciasporcategoriazona( 
   Category text,
   Descript text,
   Date date,
   Time time,
   PdDistrict text,
   Resolution text,
   Location text,
   PdId text,
   PRIMARY KEY((Category,PdDistrict),PdId)
)WITH CLUSTERING ORDER BY (PdId ASC);

-- Importar datos
COPY incidencias.incidenciasporcategoriazona (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasporcategoriazona.csv' WITH HEADER = TRUE;

-- Listar incidencias donde el distrito sea 'BAYVIEW' y la categoria sea 'SUICIDE'
SELECT COUNT(*) FROM incidencias.incidenciasporcategoriazona WHERE  Category='SUICIDE' AND pddistrict='BAYVIEW';

-- Número de incidencias donde el distrito sea 'BAYVIEW' y la categoria sea 'SUICIDE'
SELECT * FROM incidencias.incidenciasbycategoriazona WHERE Category='SUICIDE' AND pddistrict='BAYVIEW';
```


## Ver incidencias por categoria, distrito y fecha
```sql
-- Creación tabla
CREATE TABLE incidencias.incidenciasporcategoriazonafecha( 
   Category text,
   Descript text,
   Date date,
   Time time,
   PdDistrict text,
   Resolution text,
   Location text,
   PdId text,
   PRIMARY KEY((Category,PdDistrict),date,PdId)
)WITH CLUSTERING ORDER BY (date DESC, PdId ASC);

-- Importar datos
COPY incidencias.incidenciasporcategoriazonafecha (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasporcategoriazona.csv' WITH HEADER = TRUE;

-- Número de incidencias donde el distrito sea 'BAYVIEW', la categoria sea 'SUICIDE' y la fecha esté comprendida entre el 1 de junio de 2017 y el 1 de agosto de 2018 
SELECT COUNT(*) FROM incidencias.incidenciasporcategoriazonafecha WHERE Category='SUICIDE' AND pddistrict='BAYVIEW' AND date >= '2017-06-01' AND date <= '2018-08-01';
```
