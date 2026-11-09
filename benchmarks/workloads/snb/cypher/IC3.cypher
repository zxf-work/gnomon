// IC3. Deviation: + VIS on every variable, the KNOWS*1..2 walk as a named path (VISALL).
MATCH (countryX:Country {name: $countryXName }),
      (countryY:Country {name: $countryYName }),
      (person:Person {id: $personId })
WHERE VIS(countryX, countryY, person)
WITH person, countryX, countryY
LIMIT 1
MATCH (city:City)-[po:IS_PART_OF]->(country:Country)
WHERE country IN [countryX, countryY] AND VIS(city, po, country)
WITH person, countryX, countryY, collect(city) AS cities
MATCH w = (person)-[:KNOWS*1..2]-(friend), (friend)-[fl:IS_LOCATED_IN]->(city)
WHERE NOT person = friend AND NOT city IN cities AND VISALL(nodes(w)) AND VISALL(relationships(w)) AND VIS(fl, city)
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[h:HAS_CREATOR]-(message),
      (message)-[ml:IS_LOCATED_IN]->(country)
WHERE $endDate > message.creationDate >= $startDate AND
      country IN [countryX, countryY] AND VIS(h, message, ml)
WITH friend,
     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount>0 AND yCount>0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
