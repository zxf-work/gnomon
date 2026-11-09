// BI9 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI9.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH (person:Person)<-[hc:HAS_CREATOR]-(post:Post)
WHERE post.creationDate >= $startDate AND post.creationDate <= $endDate AND W_OK(person, hc, post)
WITH DISTINCT person, post
OPTIONAL MATCH w = (post)<-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE WPRUNE(_r, _n))]-(reply:Post:Comment)
WHERE reply.creationDate >= $startDate AND reply.creationDate <= $endDate AND W_OK(| nodes(w) + rels(w))
WITH person, post, count(DISTINCT reply) AS msgs
RETURN person.id, person.firstName, person.lastName, count(DISTINCT post) AS threadCount, sum(msgs) AS messageCount
ORDER BY messageCount DESC, person.id ASC
LIMIT 100
