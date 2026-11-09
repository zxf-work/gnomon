// IC6, window setting (rule W1): one binding of the post, both tag edges, both tags and a co-valid friend walk to
// the post's author: the walk is tested over the joint interval (G11 verdict 1).
MATCH (f:Person)<-[h:HAS_CREATOR]-(post:Post)-[h2:HAS_TAG]->(tag:Tag)
WHERE tag.name <> $tagName AND f.id <> $personId AND W_OK(h, post, h2, tag)
WITH f, h, post, h2, tag, W_LOV(h, post, h2, tag) AS lo, W_HIV(h, post, h2, tag) AS hi
MATCH (post)-[h1:HAS_TAG]->(t:Tag {name: $tagName})
WHERE W_INV(lo, hi, h1, t)
WITH DISTINCT f, post, tag, W_LOV(h, post, h2, tag, h1, t) AS lo, W_HIV(h, post, h2, tag, h1, t) AS hi
MATCH w = (person:Person { id: $personId })-[:KNOWS*1..2]-(f)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH DISTINCT post, tag
RETURN tag.name AS tagName, count(post) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
