// BI12 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI12.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH (person:Person)
WHERE W_OK(person)
WITH person, W_LOV(person) AS lo, W_HIV(person) AS hi
OPTIONAL MATCH (person)<-[hc:HAS_CREATOR]-(message:Post:Comment), w = (message)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE WPRUNE(_r, _n))]->(post:Post)
WHERE message.content IS NOT NULL AND message.length < $lengthThreshold AND message.creationDate > $startDate
  AND post.language IN $languages AND W_INV(lo, hi, hc, message | nodes(w) + rels(w))
WITH person, count(DISTINCT message) AS messageCount
RETURN messageCount, count(person) AS personCount
ORDER BY personCount DESC, messageCount DESC
