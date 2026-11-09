// IC13 for Memgraph (BFS with a filter lambda at each candidate instant), window setting (rule W1): the minimum, over the instants t of W, of the shortest-path length at t. The
// minimum is reached at VA or at the start of some person or friendship inside W, so those are the candidates.
CALL {
  RETURN $VA AS t
  UNION
  MATCH (x:Person) WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
  UNION
  MATCH ()-[x:KNOWS]->() WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
}
WITH t
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
WHERE VISAT(t; person1, person2)
OPTIONAL MATCH path = (person1)-[:KNOWS *BFS (e, n | VISAT(t; e, n))]-(person2)
WITH min(CASE WHEN path IS NULL THEN null ELSE size(relationships(path)) END) AS d
RETURN CASE WHEN d IS NULL THEN -1 ELSE d END AS shortestPathLength
