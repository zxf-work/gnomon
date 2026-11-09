// HAND BI14 for LadybugDB (window, rule W1), from cypher/window/BI14.cypher. Changes (D-W3-13): each score part
// ("some co-valid interaction of this kind exists": Neo4j's OPTIONAL MATCH, count, CASE count = 0) is an OPTIONAL
// MATCH without WHERE whose co-valid test is a flag the count reads (a failing row adds nothing, as in Neo4j's
// OPTIONAL MATCH ... WHERE; COUNT {} with the binding's lo / hi raised "Cannot evaluate expression with type
// PROPERTY"); its last node is a fresh variable tested against person2's key in the flag (with person2 bound at
// both ends LadybugDB returned one of two parallel LIKES copies, so a believed copy was missed: our text, worked
// around, not reproduced minimally); the score is computed inside max() (LB-7); the top pair per city by UNWIND and
// WHERE, without lambdas (LB-10): the maximum score, then the smallest person1 id, then the smallest person2 id;
// relationship uniqueness as Neo4j's (R6).
MATCH
  (country1:Country {name: $country1})<-[ip1:IS_PART_OF]-(city1:City)<-[il1:IS_LOCATED_IN]-(person1:Person),
  (country2:Country {name: $country2})<-[ip2:IS_PART_OF]-(city2:City)<-[il2:IS_LOCATED_IN]-(person2:Person),
  (person1)-[k:KNOWS]-(person2)
WHERE ip1.rid <> ip2.rid AND il1.rid <> il2.rid AND W_OK(country1, ip1, city1, il1, person1, country2, ip2, city2, il2, person2, k)
WITH person1, person2, city1, W_LOV(country1, ip1, city1, il1, person1, country2, ip2, city2, il2, person2, k) AS lo,
     W_HIV(country1, ip1, city1, il1, person1, country2, ip2, city2, il2, person2, k) AS hi
OPTIONAL MATCH (person1)<-[a1:HAS_CREATOR]-(c:Comment)-[a2:REPLY_OF]->(a3:Post:Comment)-[a4:HAS_CREATOR]->(q1:Person)
WITH person1, person2, city1, lo, hi, c, (q1.gid = person2.gid AND a1.rid <> a4.rid AND W_INV(lo, hi, a1, c, a2, a3, a4)) AS f1
WITH person1, person2, city1, lo, hi, count(CASE WHEN f1 THEN c.gid ELSE NULL END) AS n1
OPTIONAL MATCH (person1)<-[b1:HAS_CREATOR]-(m2:Post:Comment)<-[b2:REPLY_OF]-(b3:Comment)-[b4:HAS_CREATOR]->(q2:Person)
WITH person1, person2, city1, lo, hi, n1, m2, (q2.gid = person2.gid AND b1.rid <> b4.rid AND W_INV(lo, hi, b1, m2, b2, b3, b4)) AS f2
WITH person1, person2, city1, lo, hi, n1, count(CASE WHEN f2 THEN m2.gid ELSE NULL END) AS n2
OPTIONAL MATCH (person1)-[c1:LIKES]->(m3:Post:Comment)-[c2:HAS_CREATOR]->(q3:Person)
WITH person1, person2, city1, lo, hi, n1, n2, m3, (q3.gid = person2.gid AND W_INV(lo, hi, c1, m3, c2)) AS f3
WITH person1, person2, city1, lo, hi, n1, n2, count(CASE WHEN f3 THEN m3.gid ELSE NULL END) AS n3
OPTIONAL MATCH (person1)<-[d1:HAS_CREATOR]-(m4:Post:Comment)<-[d2:LIKES]-(q4:Person)
WITH person1, person2, city1, n1, n2, n3, m4, (q4.gid = person2.gid AND W_INV(lo, hi, d1, m4, d2)) AS f4
WITH person1, person2, city1, n1, n2, n3, count(CASE WHEN f4 THEN m4.gid ELSE NULL END) AS n4
WITH person1.id AS p1id, person2.id AS p2id, city1.gid AS cg, city1.name AS cname,
     max((CASE WHEN n1 > 0 THEN 4 ELSE 0 END) + (CASE WHEN n2 > 0 THEN 1 ELSE 0 END) + (CASE WHEN n3 > 0 THEN 10 ELSE 0 END) + (CASE WHEN n4 > 0 THEN 1 ELSE 0 END)) AS score
WITH cg, cname, max(score) AS ms, collect([CAST(score AS INT64), p1id, p2id]) AS rs
UNWIND rs AS r
WITH cg, cname, ms, r
WHERE r[1] = ms
WITH cg, cname, ms, min(r[2]) AS p1, collect(r) AS rs2
UNWIND rs2 AS r2
WITH cg, cname, ms, p1, r2
WHERE r2[2] = p1
WITH cg, cname, ms, p1, min(r2[3]) AS p2
RETURN p1 AS `top.person1Id`, p2 AS `top.person2Id`, cname AS `city1.name`, ms AS `top.score`
ORDER BY `top.score` DESC, `top.person1Id` ASC, `top.person2Id` ASC
LIMIT 100
