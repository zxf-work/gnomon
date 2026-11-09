// IC14 (v2) for Memgraph. Deviation: the reference projects a GDS graph; here the interaction weight of every
// visible friendship is set as a property inside the query's own transaction (rolled back), then Memgraph's
// weighted shortest path runs over visible friendships that have a weight. + VIS everywhere. Compared on the
// path weight (the path is one of possibly several).
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person), (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB)
WHERE pA.id < pB.id AND VIS(pA, knows, pB, h1, m1, r, m2, h2)
WITH knows, count(r) AS n
SET knows.w3w = CASE WHEN round(40 - sqrt(n)) > 1 THEN round(40 - sqrt(n)) ELSE 1 END
// ---
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
WHERE VIS(p1, p2)
MATCH path = (p1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VIS(e, n) AND e.w3w IS NOT NULL)]-(p2)
RETURN [x IN nodes(path) | x.id] AS personIdsInPath, total AS pathWeight
