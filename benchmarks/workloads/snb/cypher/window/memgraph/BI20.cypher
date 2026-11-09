// BI20 for Memgraph, window setting (rule W1): PathQ20 weights as a stage over valid bindings in W, set in the query's
// own transaction (rolled back); the targets are a stage over valid (company, workAt) bindings; per worker the minimum
// over candidate instants of the weighted shortest path to person2; the workers at the minimum.
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
WHERE personA.id < personB.id AND W_OK(k, saA, u, saB)
WITH k, min(abs(saA.classYear - saB.classYear)) + 1 AS w
SET k.w3w = w
// ---
MATCH (company:Company {name: $company})<-[wa:WORK_AT]-(person1:Person)
WHERE person1.id <> $person2Id AND W_OK(company, wa)
WITH DISTINCT person1
CALL {
  RETURN $VA AS t
  UNION
  MATCH (x:Person) WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
  UNION
  MATCH ()-[x:KNOWS]->() WHERE $VA < x.vt_s AND x.vt_s < $VB AND x.tt_s <= $T AND $T < x.tt_e RETURN x.vt_s AS t
}
WITH t, person1
MATCH (person2:Person {id: $person2Id})
WHERE VISAT(t; person1, person2)
MATCH path = (person1)-[:KNOWS *WSHORTEST (e, n | toFloat(coalesce(e.w3w, 0))) total (e, n | VISAT(t; e, n) AND e.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, min(total) AS totalWeight
ORDER BY totalWeight ASC
WITH collect({person1Id: person1Id, totalWeight: totalWeight}) AS results
UNWIND results AS result
WITH result.person1Id AS person1Id, result.totalWeight AS totalWeight, results
WHERE totalWeight = results[0].totalWeight
RETURN person1Id, totalWeight
ORDER BY person1Id ASC
LIMIT 20
