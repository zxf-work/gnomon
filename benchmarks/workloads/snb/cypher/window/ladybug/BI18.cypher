// BI18 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI18.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R6 (i1.rid <> i2.rid, k1.rid <> k2.rid).
MATCH (tag:Tag {name: $tag})<-[i1:HAS_INTEREST]-(person1:Person)-[k1:KNOWS]-(mutualFriend:Person)-[k2:KNOWS]-(person2:Person)-[i2:HAS_INTEREST]->(tag)
WHERE i1.rid <> i2.rid AND k1.rid <> k2.rid AND person1 <> person2 AND W_OK(tag, i1, k1, k2, i2)
WITH person1, person2, k1, W_LOV(tag, i1, k1, k2, i2) AS lo, W_HIV(tag, i1, k1, k2, i2) AS hi
OPTIONAL MATCH (person1)-[k3:KNOWS]-(person2)
WHERE W_INV(lo, hi, k3)
WITH person1, person2, k1, lo, hi, collect(CASE WHEN k3 IS NULL THEN null ELSE W_EXT(k3) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN person1.id AS person1Id, person2.id AS person2Id, count(k1) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20
