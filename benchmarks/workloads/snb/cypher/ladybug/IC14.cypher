// HAND IC14 (v2) for LadybugDB (instant settings), from cypher/memgraph/IC14.cypher (the Neo4j text projects a GDS
// graph). Changes (D-W3-13): the interaction weights are set on the visible friendships inside the call's own
// transaction, which is rolled back (as Memgraph's text does); the path is LadybugDB's weighted shortest path
// `* WSHORTEST(w3w)` over visible friendships that carry a weight. Compared on the path weight (COMPARE_LAST).
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person), (pA)<-[h1:HAS_CREATOR]-(m1:Post:Comment)-[r:REPLY_OF]-(m2:Post:Comment)-[h2:HAS_CREATOR]->(pB)
WHERE pA.id < pB.id AND h1.rid <> h2.rid AND VIS(pA, knows, pB, h1, m1, r, m2, h2)
WITH knows, count(r) AS n
SET knows.w3w = CASE WHEN round(40 - sqrt(n), 0) > 1 THEN round(40 - sqrt(n), 0) ELSE 1.0 END
// ---
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
WHERE VIS(p1, p2)
MATCH (p1)-[e:KNOWS* WSHORTEST(w3w) (_r, _n | WHERE VIS(_r, _n) AND _r.w3w IS NOT NULL)]-(p2)
RETURN list_transform(nodes(e), x -> x.id) AS personIdsInPath, cost(e) AS pathWeight
