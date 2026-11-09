// BI19 for Memgraph, window setting (rule W1): PathQ19 weights as a stage over valid bindings in W, set in the query's
// own transaction (rolled back); per (person1, person2) pair the minimum over candidate instants of the weighted
// shortest path; the pairs at the global minimum.
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)<-[h1:HAS_CREATOR]-(m1:Message)-[replyOf:REPLY_OF]-(m2:Message)-[h2:HAS_CREATOR]->(personB)
WHERE personA.id < personB.id AND W_OK(k, h1, m1, replyOf, m2, h2)
WITH k, count(replyOf) AS n
SET k.w3w = CASE WHEN round(40 - sqrt(n)) > 1 THEN round(40 - sqrt(n)) ELSE 1 END
// ---
CALL {
  RETURN $VA AS t
  UNION
  MATCH (x:Person) WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
  UNION
  MATCH ()-[x:KNOWS]->() WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
}
WITH t
MATCH (person1:Person)-[l1:IS_LOCATED_IN]->(city1:City {id: $city1Id}), (person2:Person)-[l2:IS_LOCATED_IN]->(city2:City {id: $city2Id})
WHERE VISAT(t; person1, l1, city1, person2, l2, city2)
MATCH path = (person1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VISAT(t; e, n) AND e.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, person2.id AS person2Id, min(total) AS totalWeight
ORDER BY totalWeight ASC, person1Id ASC, person2Id ASC
WITH collect({person1Id: person1Id, person2Id: person2Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.person2Id AS person2Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, person2Id, totalWeight
ORDER BY person1Id, person2Id
