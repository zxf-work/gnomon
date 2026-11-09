// IC4, window setting (rule W1): one row per co-valid (person, friendship, friend, creator edge, post, tag edge, tag)
// binding with the post in the period; "no earlier post by a friend with the tag" is W1's NOT (W_UNCOVERED).
MATCH (person:Person {id: $personId })-[k:KNOWS]-(friend:Person),
      (friend)<-[h:HAS_CREATOR]-(post:Post)-[ht:HAS_TAG]->(tag)
WHERE $endDate > post.creationDate >= $startDate AND W_OK(person, k, friend, h, post, ht, tag)
WITH tag, post, W_LOV(person, k, friend, h, post, ht, tag) AS lo, W_HIV(person, k, friend, h, post, ht, tag) AS hi
OPTIONAL MATCH (p0:Person {id: $personId})-[k0:KNOWS]-(f0:Person)<-[h0:HAS_CREATOR]-(m0:Post)-[t0:HAS_TAG]->(tag)
WHERE m0.creationDate < $startDate AND W_INV(lo, hi, k0, h0, m0, t0)
WITH tag, post, lo, hi, collect(CASE WHEN m0 IS NULL THEN null ELSE W_EXT(k0, h0, m0, t0) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN tag.name AS tagName, count(*) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
