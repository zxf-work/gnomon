// IC3 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC3.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R3, R5.
MATCH (friend:Person)-[fl:IS_LOCATED_IN]->(city:City)-[ip:IS_PART_OF]->(country:Country)
WHERE friend.id <> $personId AND NOT country.name IN [$countryXName, $countryYName] AND W_OK(friend, fl, city, ip, country)
WITH friend, fl, city, ip, country, W_LOV(friend, fl, city, ip, country) AS lo, W_HIV(friend, fl, city, ip, country) AS hi
MATCH w = (person:Person {id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH friend, collect(DISTINCT {lo: W_LOV(friend, fl, city, ip, country | nodes(w) + rels(w)),
                              hi: W_HIV(friend, fl, city, ip, country | nodes(w) + rels(w))}) AS walks
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(message:Post:Comment)-[ml:IS_LOCATED_IN]->(cx:Country {name: $countryXName})
WHERE message.creationDate < $endDate AND message.creationDate >= $startDate AND any(wk IN walks WHERE W_INV(wk.lo, wk.hi, h, message, ml, cx))
WITH friend, walks, count(DISTINCT message) AS xCount
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(message:Post:Comment)-[ml:IS_LOCATED_IN]->(cy:Country {name: $countryYName})
WHERE message.creationDate < $endDate AND message.creationDate >= $startDate AND any(wk IN walks WHERE W_INV(wk.lo, wk.hi, h, message, ml, cy))
WITH friend, xCount, count(DISTINCT message) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
