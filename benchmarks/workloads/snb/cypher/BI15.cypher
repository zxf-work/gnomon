// BI15, Neo4j with GDS, as LDBC's text: + VIS in the projection queries (parameters); one projection per call.
CALL gds.graph.project.cypher(
  $gname,
  'MATCH (p:Person) WHERE VIS(p) RETURN id(p) AS id',
  'MATCH (pA:Person)-[knows:KNOWS]-(pB:Person) WHERE VIS(pA, knows, pB)
      OPTIONAL MATCH (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB)
      WHERE VIS(h1, m1, r, m2, h2)
      OPTIONAL MATCH w = (m1)-[:REPLY_OF*0..]->(post:Post)<-[co:CONTAINER_OF]-(forum:Forum)
              WHERE forum.creationDate >= $startDate AND forum.creationDate <= $endDate
                AND VISALL(nodes(w)) AND VISALL(relationships(w))
      WITH pA, pB, sum(CASE forum IS NOT NULL WHEN true THEN CASE (m1:Post OR m2:Post) WHEN true THEN 1.0 ELSE 0.5 END ELSE 0.0 END) AS w
      RETURN id(pA) AS source, id(pB) AS target, 1/(w+1) AS weight',
  {parameters: {T: $T, V: $V, startDate: $startDate, endDate: $endDate}}
)
YIELD graphName
WITH graphName
CALL {
  WITH graphName
  MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
  WHERE VIS(person1, person2)
  CALL gds.shortestPath.dijkstra.stream(graphName, {sourceNode: person1, targetNode: person2, relationshipWeightProperty: 'weight'})
  YIELD totalCost
  RETURN totalCost
  UNION ALL
  WITH graphName
  RETURN -1.0 AS totalCost
}
RETURN max(totalCost) AS totalCost
