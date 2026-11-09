// HAND IC3 for LadybugDB (instant settings), from cypher/IC3.cypher. Changes (D-W3-13): node identity in list tests
// and equality is on the node key gid (LadybugDB refuses `country IN [countryX, countryY]`: "Casting between NODE and
// NODE is not implemented"); the chained comparison is two comparisons; the walk as R5.
MATCH (countryX:Country {name: $countryXName }),
      (countryY:Country {name: $countryYName }),
      (person:Person {id: $personId })
WHERE VIS(countryX, countryY, person)
WITH person, countryX, countryY
LIMIT 1
MATCH (city:City)-[po:IS_PART_OF]->(country:Country)
WHERE country.gid IN [countryX.gid, countryY.gid] AND VIS(city, po, country)
WITH person, countryX, countryY, collect(city.gid) AS cities
MATCH w = (person)-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE VIS(_r, _n))]-(friend:Person), (friend)-[fl:IS_LOCATED_IN]->(city:City)
WHERE NOT person.gid = friend.gid AND NOT city.gid IN cities AND VIS(person, friend) AND VIS(fl, city)
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[h:HAS_CREATOR]-(message:Post:Comment),
      (message)-[ml:IS_LOCATED_IN]->(country:Country)
WHERE message.creationDate < $endDate AND message.creationDate >= $startDate AND
      country.gid IN [countryX.gid, countryY.gid] AND VIS(h, message, ml)
WITH friend,
     CASE WHEN country.gid = countryX.gid THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.gid = countryY.gid THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
