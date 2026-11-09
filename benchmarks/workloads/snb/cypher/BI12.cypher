// BI12. Deviation: + VIS on every variable, the REPLY_OF*0.. walk as a named path (VISALL); the language of the
// root post is the root's own language property.
MATCH (person:Person)
WHERE VIS(person)
OPTIONAL MATCH (person)<-[hc:HAS_CREATOR]-(message:Message), w = (message)-[:REPLY_OF*0..]->(post:Post)
WHERE message.content IS NOT NULL
  AND message.length < $lengthThreshold
  AND message.creationDate > $startDate
  AND post.language IN $languages
  AND VIS(hc, message) AND VISALL(nodes(w)) AND VISALL(relationships(w))
WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
ORDER BY
  personCount DESC,
  messageCount DESC
