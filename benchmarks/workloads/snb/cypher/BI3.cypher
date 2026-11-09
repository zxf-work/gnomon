// BI3. Deviation: + VIS on every variable, the REPLY_OF*0.. walk as a named path (VISALL).
MATCH
  (co:Country {name: $country})<-[ip:IS_PART_OF]-(ci:City)<-[il:IS_LOCATED_IN]-
  (person:Person)<-[hm:HAS_MODERATOR]-(forum:Forum)-[cf:CONTAINER_OF]->(post:Post),
  w = (post)<-[:REPLY_OF*0..]-(message:Message),
  (message)-[ht:HAS_TAG]->(t:Tag)-[hy:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(co, ip, ci, il, person, hm, forum, cf, post, ht, t, hy, tc) AND VISALL(nodes(w)) AND VISALL(relationships(w))
RETURN
  forum.id,
  forum.title,
  forum.creationDate,
  person.id,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  forum.id ASC
LIMIT 20
