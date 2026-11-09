// BI19 for Memgraph. Deviation: the PathQ19 / GDS weights are set on visible friendships inside the query's own
// transaction (rolled back); Memgraph's weighted shortest path per (person1, person2) pair; the pairs at the
// minimum are returned. + VIS everywhere.
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)<-[h1:HAS_CREATOR]-(m1:Message)-[replyOf:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(personB)
WHERE personA.id < personB.id AND VIS(personA, k, personB, h1, m1, replyOf, m2, h2)
WITH k, count(replyOf) AS n
SET k.w3w = CASE WHEN round(40 - sqrt(n)) > 1 THEN round(40 - sqrt(n)) ELSE 1 END
// ---
MATCH (person1:Person)-[l1:IS_LOCATED_IN]->(city1:City {id: $city1Id}), (person2:Person)-[l2:IS_LOCATED_IN]->(city2:City {id: $city2Id})
WHERE VIS(person1, l1, city1, person2, l2, city2)
MATCH path = (person1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VIS(e, n) AND e.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, person2.id AS person2Id, total AS totalWeight
ORDER BY totalWeight ASC, person1Id ASC, person2Id ASC
WITH collect({person1Id: person1Id, person2Id: person2Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.person2Id AS person2Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, person2Id, totalWeight
ORDER BY person1Id, person2Id
