// BI18 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI18.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R6 (i1.rid <> i2.rid, k1.rid <> k2.rid).
MATCH (tag:Tag {name: $tag})<-[i1:HAS_INTEREST]-(person1:Person)-[k1:KNOWS]-(mutualFriend:Person)-[k2:KNOWS]-(person2:Person)-[i2:HAS_INTEREST]->(tag)
WHERE i1.rid <> i2.rid AND k1.rid <> k2.rid AND person1 <> person2 AND VIS(tag, i1, person1, k1, mutualFriend, k2, person2, i2)
OPTIONAL MATCH (person1)-[k3:KNOWS]-(person2)
WHERE VIS(k3)
WITH person1, person2, mutualFriend, count(k3) AS direct
WHERE direct = 0
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20
