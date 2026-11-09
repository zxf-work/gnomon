// HAND BI20 for LadybugDB (instant settings), from cypher/memgraph/BI20.cypher (the Neo4j text projects a GDS graph).
// Changes (D-W3-13): the weights are set inside the call's own transaction, rolled back; `* WSHORTEST(w3w)`; the
// persons at the smallest total weight by min() and a filter; STUDY_AT uniqueness as Neo4j's (R6).
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
WHERE personA.id < personB.id AND saA.rid <> saB.rid AND VIS(personA, k, personB, saA, u, saB)
WITH k, min(abs(saA.classYear - saB.classYear)) + 1 AS wt
SET k.w3w = CAST(wt AS DOUBLE)
// ---
MATCH (company:Company {name: $company})<-[ww:WORK_AT]-(person1:Person), (person2:Person {id: $person2Id})
WHERE person1.id <> $person2Id AND VIS(company, ww, person1, person2)
MATCH (person1)-[e:KNOWS* WSHORTEST(w3w) (_r, _n | WHERE VIS(_r, _n) AND _r.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, cost(e) AS totalWeight
WITH min(totalWeight) AS mw, collect({p1: person1Id, w: totalWeight}) AS rs
UNWIND rs AS r
WITH r, mw
WHERE r.w = mw
RETURN r.p1 AS person1Id, r.w AS totalWeight
ORDER BY person1Id ASC
LIMIT 20
