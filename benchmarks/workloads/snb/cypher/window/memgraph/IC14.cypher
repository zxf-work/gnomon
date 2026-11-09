// IC14 (v2) for Memgraph, window setting (rule W1): the weights are a stage over valid (reply, parent, creator edges,
// friendship) bindings in W, set on the friendships inside the query's own transaction (rolled back); the answer is
// the minimum over the candidate instants t (VA and every person / friendship start in W) of the weighted shortest
// path at t. Compared on the path weight.
MATCH (pA:Person)-[knows:KNOWS]-(pB:Person), (pA)<-[h1:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(pB)
WHERE pA.id < pB.id AND W_OK(knows, h1, m1, r, m2, h2)
WITH knows, count(r) AS n
SET knows.w3w = CASE WHEN round(40 - sqrt(n)) > 1 THEN round(40 - sqrt(n)) ELSE 1 END
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
MATCH path = (p1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VISAT(t; e, n) AND e.w3w IS NOT NULL)]-(p2)
WITH min(total) AS pathWeight
WHERE pathWeight IS NOT NULL   // no path: no row, as at the instant settings (an aggregate alone gives one null row)
RETURN pathWeight
