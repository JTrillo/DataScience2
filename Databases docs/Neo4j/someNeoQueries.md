# Some Neo4j Queries 

## All districts
`MATCH (dis:District) RETURN dis`

## Tenderloin district
`MATCH (dis:District {district:'TENDERLOIN'}) RETURN dis`

## Number of incidents in Tenderloin
`MATCH (dis:District {district:'TENDERLOIN'})
RETURN size(()-[:WHERE]->(dis)) AS tenderloin_crimes`

## All categories
`MATCH (cat:Category) RETURN cat`

## Vandalism category
`MATCH (cat:Category {category:'VANDALISM'}) RETURN cat`

## Number of cases of vandalism or drunkenness
`MATCH (cat:Category {category:'VANDALISM'}), (cat2:Category {category:'DRUNKENNESS'})
RETURN size(()-[:WHAT]->(cat))+size(()-[:WHAT]->(cat2)) AS vandalism_and_drunkenness_cases`

## Number of cases of vandalism in Tenderloin
`MATCH (cat:Category {category:'VANDALISM'}), (dis:District {district:'TENDERLOIN'})
RETURN size((cat)<-[:WHAT]-()-[:WHERE]->(dis)) AS cases`

## Percentage of vandalism cases in Tenderloin
`MATCH (cat:Category {category:'VANDALISM'}), (dis:District {district:'TENDERLOIN'})
WITH toFloat(size((cat)<-[:WHAT]-()-[:WHERE]->(dis))) AS cases,
size(()-[:WHERE]->(dis)) * 1.0 AS total_cases
RETURN cases/total_cases*100 AS percentage`
