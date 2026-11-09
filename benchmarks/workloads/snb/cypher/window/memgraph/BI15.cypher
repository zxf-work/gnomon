// BI15 for Memgraph, window setting (rule W1): the weights 1/(w + 1) are a stage over valid bindings in W (the
// friendship, the reply pair and the forum walk), set in the query's own transaction (rolled back); the minimum over
// the candidate instants of the weighted shortest path at t; -1 when none.
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
WHERE pA.id < pB.id AND W_OK(knows)
OPTIONAL MATCH (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB),
      w = (m1)-[:REPLY_OF*0..]->(post:Post)<-[co:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate AND forum.creationDate <= $endDate AND W_OK(h1, m1, r, m2, h2, co | nodes(w) + relationships(w))
WITH knows, sum(CASE forum IS NOT NULL WHEN true THEN CASE (m1:Post OR m2:Post) WHEN true THEN 1.0 ELSE 0.5 END ELSE 0.0 END) AS wsum
SET knows.w3w = 1 / (wsum + 1)
// ---
CALL {
  RETURN $VA AS t
  UNION
  MATCH (x:Person) WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
  UNION
  MATCH ()-[x:KNOWS]->() WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
}
WITH t
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
WHERE VISAT(t; p1, p2)
OPTIONAL MATCH path = (p1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VISAT(t; e, n) AND e.w3w IS NOT NULL)]-(p2)
WITH min(CASE WHEN path IS NULL THEN null ELSE total END) AS best
RETURN CASE WHEN best IS NULL THEN -1.0 ELSE best END AS totalCost
