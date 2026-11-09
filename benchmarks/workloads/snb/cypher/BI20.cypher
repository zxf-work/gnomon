// BI20, Neo4j with GDS, as LDBC's text (bi-20-create-graph + bi-20): + VIS in the projection (parameters) and on the
// persons and the company; one projection per call.
CALL gds.graph.project.cypher(
  $gname,
  'MATCH (p:Person) WHERE VIS(p) RETURN id(p) AS id',
  'MATCH (personA:Person)-[k:KNOWS]-(personB:Person),
      (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
    WHERE VIS(personA, k, personB, saA, u, saB)
    RETURN id(personA) AS source, id(personB) AS target, min(abs(saA.classYear - saB.classYear)) + 1 AS weight',
  {parameters: {T: $T, V: $V}}
)
YIELD graphName
WITH graphName
MATCH
  (company:Company {name: $company})<-[w:WORK_AT]-(person1:Person),
  (person2:Person {id: $person2Id})
WHERE VIS(company, w, person1, person2)
CALL gds.shortestPath.dijkstra.stream(graphName, {sourceNode: person1, targetNode: person2, relationshipWeightProperty: 'weight'})
YIELD totalCost
WITH person1, totalCost
WHERE person1.id <> $person2Id
WITH person1.id AS person1Id, totalCost AS totalWeight
ORDER BY totalWeight ASC
WITH collect({person1Id: person1Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, totalWeight
ORDER BY person1Id ASC
LIMIT 20
