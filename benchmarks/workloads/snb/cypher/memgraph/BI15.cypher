// BI15 for Memgraph. Deviation: the reference projects a GDS graph; here every visible friendship gets its weight
// 1/(w + 1) as a property inside the query's own transaction (rolled back), then Memgraph's weighted shortest
// path runs over visible friendships. + VIS everywhere. -1 when no path.
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
WHERE pA.id < pB.id AND VIS(pA, knows, pB)
OPTIONAL MATCH (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB)
WHERE VIS(h1, m1, r, m2, h2)
OPTIONAL MATCH w = (m1)-[:REPLY_OF*0..]->(post:Post)<-[co:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate AND forum.creationDate <= $endDate AND VISALL(nodes(w)) AND VISALL(relationships(w)) AND VIS(co)
WITH knows, sum(CASE forum IS NOT NULL WHEN true THEN CASE (m1:Post OR m2:Post) WHEN true THEN 1.0 ELSE 0.5 END ELSE 0.0 END) AS wsum
SET knows.w3w = 1 / (wsum + 1)
// ---
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
WHERE VIS(p1, p2)
OPTIONAL MATCH path = (p1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VIS(e, n) AND e.w3w IS NOT NULL)]-(p2)
RETURN CASE WHEN path IS NULL THEN -1.0 ELSE total END AS totalCost
