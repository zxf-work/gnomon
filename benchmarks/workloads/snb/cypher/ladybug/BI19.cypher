// HAND BI19 for LadybugDB (instant settings), from cypher/memgraph/BI19.cypher (the Neo4j text projects a GDS graph).
// Changes (D-W3-13): interaction weights set inside the call's own transaction, rolled back; `* WSHORTEST(w3w)`;
// the pairs at the smallest total weight are kept by min() and a filter (LadybugDB keeps no order through
// collect); relationship uniqueness as Neo4j's (R6).
MATCH (personA:Person)-[k:KNOWS]-(personB:Person), (personA)<-[h1:HAS_CREATOR]-(m1:Post:Comment)-[replyOf:REPLY_OF]-(m2:Post:Comment)-[h2:HAS_CREATOR]->(personB)
WHERE personA.id < personB.id AND h1.rid <> h2.rid AND VIS(personA, k, personB, h1, m1, replyOf, m2, h2)
WITH k, count(replyOf) AS n
SET k.w3w = CASE WHEN round(40 - sqrt(n), 0) > 1 THEN round(40 - sqrt(n), 0) ELSE 1.0 END
// ---
MATCH (person1:Person)-[l1:IS_LOCATED_IN]->(city1:City {id: $city1Id}), (person2:Person)-[l2:IS_LOCATED_IN]->(city2:City {id: $city2Id})
WHERE l1.rid <> l2.rid AND VIS(person1, l1, city1, person2, l2, city2)
MATCH (person1)-[e:KNOWS* WSHORTEST(w3w) (_r, _n | WHERE VIS(_r, _n) AND _r.w3w IS NOT NULL)]-(person2)
WITH person1.id AS person1Id, person2.id AS person2Id, cost(e) AS totalWeight
WITH min(totalWeight) AS mw, collect({p1: person1Id, p2: person2Id, w: totalWeight}) AS rs
UNWIND rs AS r
WITH r, mw
WHERE r.w = mw
RETURN r.p1 AS person1Id, r.p2 AS person2Id, r.w AS totalWeight
ORDER BY person1Id, person2Id
