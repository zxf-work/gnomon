// BI12, window setting (rule W1): per person version, the count of its messages co-valid with it (the optional part,
// with the REPLY_OF*0.. walk to the root post); then the distribution.
MATCH (person:Person)
WHERE W_OK(person)
WITH person, W_LOV(person) AS lo, W_HIV(person) AS hi
OPTIONAL MATCH (person)<-[hc:HAS_CREATOR]-(message:Message), w = (message)-[:REPLY_OF*0..]->(post:Post)
WHERE message.content IS NOT NULL AND message.length < $lengthThreshold AND message.creationDate > $startDate
  AND post.language IN $languages AND W_INV(lo, hi, hc, message | nodes(w) + relationships(w))
WITH person, count(DISTINCT message) AS messageCount
RETURN messageCount, count(person) AS personCount
ORDER BY personCount DESC, messageCount DESC
