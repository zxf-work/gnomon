// BI3 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI3.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH
  (co:Country {name: $country})<-[ip:IS_PART_OF]-(ci:City)<-[il:IS_LOCATED_IN]-
  (person:Person)<-[hm:HAS_MODERATOR]-(forum:Forum)-[cf:CONTAINER_OF]->(post:Post),
  w = (post)<-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]-(message:Post:Comment),
  (message)-[ht:HAS_TAG]->(t:Tag)-[hy:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE VIS(co, ip, ci, il, person, hm, forum, cf, post, ht, t, hy, tc) AND VIS(post, message)
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
