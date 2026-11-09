// BI19, Neo4j with GDS, as LDBC's text (bi-19-create-graph + bi-19): + VIS in the projection (parameters) and on the
// end persons; one projection per call.
CALL gds.graph.project.cypher(
  $gname,
  'MATCH (p:Person) WHERE VIS(p) RETURN id(p) AS id',
  'MATCH (personA:Person)-[k:KNOWS]-(personB:Person),
      (personA)<-[h1:HAS_CREATOR]-(m1:Message)-[replyOf:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(personB)
    WHERE VIS(personA, k, personB, h1, m1, replyOf, m2, h2)
    WITH id(personA) AS source, id(personB) AS target, count(replyOf) AS numInteractions
    RETURN source, target, CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight',
  {parameters: {T: $T, V: $V}}
)
YIELD graphName
WITH graphName
MATCH
  (person1:Person)-[l1:IS_LOCATED_IN]->(city1:City {id: $city1Id}),
  (person2:Person)-[l2:IS_LOCATED_IN]->(city2:City {id: $city2Id})
WHERE VIS(person1, l1, city1, person2, l2, city2)
CALL gds.shortestPath.dijkstra.stream(graphName, {sourceNode: person1, targetNode: person2, relationshipWeightProperty: 'weight'})
YIELD totalCost
WITH person1.id AS person1Id, person2.id AS person2Id, totalCost AS totalWeight
ORDER BY totalWeight ASC, person1Id ASC, person2Id ASC
WITH collect({person1Id: person1Id, person2Id: person2Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.person2Id AS person2Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, person2Id, totalWeight
ORDER BY person1Id, person2Id
