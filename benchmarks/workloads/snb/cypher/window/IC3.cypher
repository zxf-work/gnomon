// IC3, window setting (rule W1): the friend walk (1..2 hops) co-valid with the (friend, city, country) binding, carried
// with its own interval; the two counts are correlated parts of that binding (G11 verdict 3): a message counts when it
// shares an instant with the friend, the city, the country and some co-valid walk.
MATCH (friend:Person)-[fl:IS_LOCATED_IN]->(city:City)-[ip:IS_PART_OF]->(country:Country)
WHERE friend.id <> $personId AND NOT country.name IN [$countryXName, $countryYName] AND W_OK(friend, fl, city, ip, country)
WITH friend, fl, city, ip, country, W_LOV(friend, fl, city, ip, country) AS lo, W_HIV(friend, fl, city, ip, country) AS hi
MATCH w = (person:Person {id: $personId })-[:KNOWS*1..2]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH friend, collect(DISTINCT {lo: W_LOV(friend, fl, city, ip, country | nodes(w) + relationships(w)),
                              hi: W_HIV(friend, fl, city, ip, country | nodes(w) + relationships(w))}) AS walks
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(message:Message)-[ml:IS_LOCATED_IN]->(cx:Country {name: $countryXName})
WHERE $endDate > message.creationDate >= $startDate AND any(wk IN walks WHERE W_INV(wk.lo, wk.hi, h, message, ml, cx))
WITH friend, walks, count(DISTINCT message) AS xCount
OPTIONAL MATCH (friend)<-[h:HAS_CREATOR]-(message:Message)-[ml:IS_LOCATED_IN]->(cy:Country {name: $countryYName})
WHERE $endDate > message.creationDate >= $startDate AND any(wk IN walks WHERE W_INV(wk.lo, wk.hi, h, message, ml, cy))
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
