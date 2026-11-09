// HAND BI15 for LadybugDB (instant settings), from cypher/memgraph/BI15.cypher (the Neo4j text projects a GDS graph).
// Changes (D-W3-13): the weights 1/(w + 1) are set on every visible friendship inside the call's own transaction,
// rolled back (as Memgraph's text); the path is `* WSHORTEST(w3w)` over visible weighted friendships; the label test
// (m1:Post OR m2:Post) is label() = 'Post'; the walk as R5; HAS_CREATOR uniqueness as Neo4j's (R6). No path: -1.
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
WHERE pA.id < pB.id AND VIS(pA, knows, pB)
OPTIONAL MATCH (pA)<-[h1:HAS_CREATOR]-(m1:Post:Comment)-[r:REPLY_OF]-(m2:Post:Comment)-[h2:HAS_CREATOR]->(pB)
WHERE h1.rid <> h2.rid AND VIS(h1, m1, r, m2, h2)
OPTIONAL MATCH w = (m1)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(post:Post)<-[co:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate AND forum.creationDate <= $endDate AND VIS(m1, post) AND VIS(co)
WITH knows, sum(CASE WHEN forum IS NULL THEN 0.0 WHEN label(m1) = 'Post' OR label(m2) = 'Post' THEN 1.0 ELSE 0.5 END) AS wsum
SET knows.w3w = 1.0 / (wsum + 1.0)
// ---
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
WHERE VIS(p1, p2)
OPTIONAL MATCH (p1)-[e:KNOWS* WSHORTEST(w3w) (_r, _n | WHERE VIS(_r, _n) AND _r.w3w IS NOT NULL)]-(p2)
RETURN CASE WHEN e IS NULL THEN -1.0 ELSE cost(e) END AS totalCost
