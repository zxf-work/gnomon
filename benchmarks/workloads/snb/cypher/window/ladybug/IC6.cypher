// IC6 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC6.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (f:Person)<-[h:HAS_CREATOR]-(post:Post)-[h2:HAS_TAG]->(tag:Tag)
WHERE tag.name <> $tagName AND f.id <> $personId AND W_OK(h, post, h2, tag)
WITH f, h, post, h2, tag, W_LOV(h, post, h2, tag) AS lo, W_HIV(h, post, h2, tag) AS hi
MATCH (post)-[h1:HAS_TAG]->(t:Tag {name: $tagName})
WHERE W_INV(lo, hi, h1, t)
WITH DISTINCT f, post, tag, W_LOV(h, post, h2, tag, h1, t) AS lo, W_HIV(h, post, h2, tag, h1, t) AS hi
MATCH w = (person:Person { id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(f)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH DISTINCT post, tag
RETURN tag.name AS tagName, count(post) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
