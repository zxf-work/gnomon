// HAND BI14 for LadybugDB (instant settings), from cypher/BI14.cypher. Changes (D-W3-13): the top pair per city is
// collect(...)[0] after ORDER BY city, score DESC, person1.id, person2.id in Neo4j; LadybugDB keeps no order through an
// aggregation, so it is the maximum score per city, then the smallest person1 id, then the smallest person2 id
// among the city's pairs (list_reduce over the collected [score, person1, person2] keys); relationship uniqueness in
// the first MATCH as Neo4j's (R6).
MATCH
  (country1:Country {name: $country1})<-[ip1:IS_PART_OF]-(city1:City)<-[il1:IS_LOCATED_IN]-(person1:Person),
  (country2:Country {name: $country2})<-[ip2:IS_PART_OF]-(city2:City)<-[il2:IS_LOCATED_IN]-(person2:Person),
  (person1)-[k:KNOWS]-(person2)
WHERE ip1.rid <> ip2.rid AND il1.rid <> il2.rid AND VIS(country1, ip1, city1, il1, person1, country2, ip2, city2, il2, person2, k)
WITH person1, person2, city1, 0 AS score
OPTIONAL MATCH (person1)<-[a1:HAS_CREATOR]-(c:Comment)-[a2:REPLY_OF]->(a3:Post:Comment)-[a4:HAS_CREATOR]->(person2)
WHERE a1.rid <> a4.rid AND VIS(a1, c, a2, a3, a4)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN c IS NULL THEN 0 ELSE  4 END) AS score
OPTIONAL MATCH (person1)<-[b1:HAS_CREATOR]-(m:Post:Comment)<-[b2:REPLY_OF]-(b3:Comment)-[b4:HAS_CREATOR]->(person2)
WHERE b1.rid <> b4.rid AND VIS(b1, m, b2, b3, b4)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
OPTIONAL MATCH (person1)-[c1:LIKES]->(m:Post:Comment)-[c2:HAS_CREATOR]->(person2)
WHERE VIS(c1, m, c2)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE 10 END) AS score
OPTIONAL MATCH (person1)<-[d1:HAS_CREATOR]-(m:Post:Comment)<-[d2:LIKES]-(person2)
WHERE VIS(d1, m, d2)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
WITH city1, max(score) AS ms, collect([CAST(score AS INT64), person1.id, person2.id]) AS rs
WITH city1, ms, list_filter(rs, x -> x[1] = ms) AS top
WITH city1, ms, top, list_reduce(list_transform(top, x -> x[2]), (a, b) -> CASE WHEN b < a THEN b ELSE a END) AS p1
WITH city1, ms, p1, list_reduce(list_transform(list_filter(top, x -> x[2] = p1), x -> x[3]), (a, b) -> CASE WHEN b < a THEN b ELSE a END) AS p2
RETURN p1 AS `top.person1Id`, p2 AS `top.person2Id`, city1.name AS `city1.name`, ms AS `top.score`
ORDER BY `top.score` DESC, `top.person1Id` ASC, `top.person2Id` ASC
LIMIT 100
