// BI9 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI9.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH (person:Person)<-[hc:HAS_CREATOR]-(post:Post), w = (post)<-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]-(reply:Post:Comment)
WHERE  post.creationDate >= $startDate
  AND  post.creationDate <= $endDate
  AND reply.creationDate >= $startDate
  AND reply.creationDate <= $endDate
  AND VIS(person, hc, post) AND VIS(post, reply)
RETURN
  person.id,
  person.firstName,
  person.lastName,
  count(DISTINCT post) AS threadCount,
  count(DISTINCT reply) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100
