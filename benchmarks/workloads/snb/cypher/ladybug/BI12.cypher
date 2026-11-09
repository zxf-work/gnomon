// BI12 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI12.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH (person:Person)
WHERE VIS(person)
OPTIONAL MATCH (person)<-[hc:HAS_CREATOR]-(message:Post:Comment), w = (message)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(post:Post)
WHERE message.content IS NOT NULL
  AND message.length < $lengthThreshold
  AND message.creationDate > $startDate
  AND post.language IN $languages
  AND VIS(hc, message) AND VIS(message, post)
WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
ORDER BY
  personCount DESC,
  messageCount DESC
