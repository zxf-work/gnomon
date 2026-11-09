// BI14. Deviation: + VIS on every variable.
MATCH
  (country1:Country {name: $country1})<-[ip1:IS_PART_OF]-(city1:City)<-[il1:IS_LOCATED_IN]-(person1:Person),
  (country2:Country {name: $country2})<-[ip2:IS_PART_OF]-(city2:City)<-[il2:IS_LOCATED_IN]-(person2:Person),
  (person1)-[k:KNOWS]-(person2)
WHERE VIS(country1, ip1, city1, il1, person1, country2, ip2, city2, il2, person2, k)
WITH person1, person2, city1, 0 AS score
OPTIONAL MATCH (person1)<-[a1:HAS_CREATOR]-(c:Comment)-[a2:REPLY_OF]->(a3:Message)-[a4:HAS_CREATOR]->(person2)
WHERE VIS(a1, c, a2, a3, a4)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN c IS NULL THEN 0 ELSE  4 END) AS score
OPTIONAL MATCH (person1)<-[b1:HAS_CREATOR]-(m:Message)<-[b2:REPLY_OF]-(b3:Comment)-[b4:HAS_CREATOR]->(person2)
WHERE VIS(b1, m, b2, b3, b4)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
OPTIONAL MATCH (person1)-[c1:LIKES]->(m:Message)-[c2:HAS_CREATOR]->(person2)
WHERE VIS(c1, m, c2)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE 10 END) AS score
OPTIONAL MATCH (person1)<-[d1:HAS_CREATOR]-(m:Message)<-[d2:LIKES]-(person2)
WHERE VIS(d1, m, d2)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
ORDER BY
  city1.name ASC,
  score DESC,
  person1.id ASC,
  person2.id ASC
WITH city1, collect({score: score, person1Id: person1.id, person2Id: person2.id})[0] AS top
RETURN
  top.person1Id,
  top.person2Id,
  city1.name,
  top.score
ORDER BY
  top.score DESC,
  top.person1Id ASC,
  top.person2Id ASC
LIMIT 100
