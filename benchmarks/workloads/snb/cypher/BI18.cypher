// BI18. Deviation: + VIS on every variable; "the two are not friends" is an OPTIONAL MATCH count with VIS.
MATCH (tag:Tag {name: $tag})<-[i1:HAS_INTEREST]-(person1:Person)-[k1:KNOWS]-(mutualFriend:Person)-[k2:KNOWS]-(person2:Person)-[i2:HAS_INTEREST]->(tag)
WHERE person1 <> person2 AND VIS(tag, i1, person1, k1, mutualFriend, k2, person2, i2)
OPTIONAL MATCH (person1)-[k3:KNOWS]-(person2)
WHERE VIS(k3)
WITH person1, person2, mutualFriend, count(k3) AS direct
WHERE direct = 0
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20
