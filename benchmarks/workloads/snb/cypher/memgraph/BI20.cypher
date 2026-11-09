// BI20 for Memgraph. Deviation: the PathQ20 / GDS weights are set on visible friendships inside the query's own
// transaction (rolled back); Memgraph's weighted shortest path from each worker to person2; the persons at the
// minimum are returned. + VIS everywhere.
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
WHERE personA.id < personB.id AND VIS(personA, k, personB, saA, u, saB)
WITH k, min(abs(saA.classYear - saB.classYear)) + 1 AS w
SET k.w3w = w
// ---
MATCH (company:Company {name: $company})<-[w:WORK_AT]-(person1:Person), (person2:Person {id: $person2Id})
WHERE person1.id <> $person2Id AND VIS(company, w, person1, person2)
MATCH path = (person1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VIS(e, n) AND e.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, total AS totalWeight
ORDER BY totalWeight ASC
WITH collect({person1Id: person1Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, totalWeight
ORDER BY person1Id ASC
LIMIT 20
