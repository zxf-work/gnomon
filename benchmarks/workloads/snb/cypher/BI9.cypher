// BI9. Deviation: + VIS on every variable, the REPLY_OF*0.. walk as a named path (VISALL).
MATCH (person:Person)<-[hc:HAS_CREATOR]-(post:Post), w = (post)<-[:REPLY_OF*0..]-(reply:Message)
WHERE  post.creationDate >= $startDate
  AND  post.creationDate <= $endDate
  AND reply.creationDate >= $startDate
  AND reply.creationDate <= $endDate
  AND VIS(person, hc, post) AND VISALL(nodes(w)) AND VISALL(relationships(w))
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
