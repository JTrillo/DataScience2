#tabla con todos los campos completo
CREATE TABLE incidencias.completoPdId( 
   IncidntNum text, 
   Category text,
   Descript text,
   DayOfWeek text,
   Date date,
   Time time,
   PdDistrict text,
   Resolution text,
   Address Text,
   X text,
   Y text,
   Location text,
   PdId text,
   PRIMARY KEY(PdId)
);

COPY incidencias.completoPdId (IncidntNum,Category,Descript,DayOfWeek,Date,Time,PdDistrict,Resolution,Address,X,Y,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/completo.csv' WITH HEADER = TRUE;
select COUNT(*) from incidencias.completoPdId;


#ver actividad criminal por zonas de la ciudad
CREATE TABLE incidencias.incidenciasbyzona( 
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

COPY incidencias.incidenciasbyzona (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasbyzona.csv' WITH HEADER = TRUE;
SELECT COUNT(*) FROM incidencias.incidenciasbyzona WHERE pddistrict='NORTHERN';
SELECT * FROM incidencias.incidenciasbyzona WHERE pddistrict='NORTHERN';

#ver las categorias de incidentes distintas que hay
CREATE TABLE incidencias.categorias( 
   Category text,
   PRIMARY KEY(Category)
);

COPY incidencias.categorias (Category) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/categorias.csv' WITH HEADER = TRUE;
SELECT COUNT(*) FROM incidencias.categorias;
SELECT * FROM incidencias.categorias;


#numero de incidentes por categoria y distrito
CREATE TABLE incidencias.incidenciasbycategoriazona( 
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

COPY incidencias.incidenciasbycategoriazona (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasbycategoriazona.csv' WITH HEADER = TRUE;
SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazona WHERE  Category='SUICIDE' AND pddistrict='BAYVIEW';
SELECT * FROM incidencias.incidenciasbycategoriazona WHERE Category='SUICIDE' AND pddistrict='BAYVIEW';


#numero de incidentes por categoria, distrito y fecha
CREATE TABLE incidencias.incidenciasbycategoriazonafecha( 
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

COPY incidencias.incidenciasbycategoriazonafecha (Category,Descript,Date,Time,PdDistrict,Resolution,Location,PdId) FROM 'C:/Users/Ivan/Desktop/doccassandra/CSVs/incidenciasbycategoriazona.csv' WITH HEADER = TRUE;
SELECT COUNT(*) FROM incidencias.incidenciasbycategoriazonafecha WHERE  Category='SUICIDE' AND pddistrict='BAYVIEW' AND date >= '2017-06-01' AND date <= '2018-08-01';
