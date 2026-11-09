// IC14 (v2), Neo4j with the Graph Data Science library, as LDBC's text: + VIS in the projection queries (passed as
// parameters); one projection per call, dropped after it.
CALL gds.graph.project.cypher(
  $gname,
  'MATCH (p:Person) WHERE VIS(p) RETURN id(p) AS id',
  'MATCH (pA:Person)-[knows:KNOWS]-(pB:Person), (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB)
   WHERE VIS(pA, knows, pB, h1, m1, r, m2, h2)
   WITH id(pA) AS source, id(pB) AS target, count(r) AS numInteractions
   RETURN source, target, CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight',
  {parameters: {T: $T, V: $V}}
)
YIELD graphName
WITH graphName
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
WHERE VIS(person1, person2)
CALL gds.shortestPath.dijkstra.stream(graphName, {sourceNode: person1, targetNode: person2, relationshipWeightProperty: 'weight'})
YIELD totalCost, path
RETURN [p IN nodes(path) | p.id] AS personIdsInPath, totalCost AS pathWeight
LIMIT 1
