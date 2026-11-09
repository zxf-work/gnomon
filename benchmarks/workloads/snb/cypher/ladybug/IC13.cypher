// HAND IC13 for LadybugDB (instant settings), from cypher/IC13.cypher. Changes (D-W3-13): shortestPath((a)-[:KNOWS*]-(b))
// is `* SHORTEST 1..30` with the VIS test in the step predicate; LadybugDB bounds a recursive step (30 here, its
// default), Neo4j's is unbounded (no KNOWS shortest path in W3 is near 30 hops).
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
WHERE VIS(person1, person2)
OPTIONAL MATCH (person1)-[e:KNOWS* SHORTEST 1..30 (_r, _n | WHERE VIS(_r, _n))]-(person2)
RETURN CASE WHEN e IS NULL THEN -1 ELSE length(e) END AS shortestPathLength
