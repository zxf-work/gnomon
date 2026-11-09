// BI18, window setting (rule W1): one co-valid binding (both interests, both friendships to the mutual friend); "the
// two are not friends" is W1's NOT (W_UNCOVERED).
MATCH (tag:Tag {name: $tag})<-[i1:HAS_INTEREST]-(person1:Person)-[k1:KNOWS]-(mutualFriend:Person)-[k2:KNOWS]-(person2:Person)-[i2:HAS_INTEREST]->(tag)
WHERE person1 <> person2 AND W_OK(tag, i1, k1, k2, i2)
WITH person1, person2, k1, W_LOV(tag, i1, k1, k2, i2) AS lo, W_HIV(tag, i1, k1, k2, i2) AS hi
OPTIONAL MATCH (person1)-[k3:KNOWS]-(person2)
WHERE W_INV(lo, hi, k3)
WITH person1, person2, k1, lo, hi, collect(CASE WHEN k3 IS NULL THEN null ELSE W_EXT(k3) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN person1.id AS person1Id, person2.id AS person2Id, count(k1) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20
