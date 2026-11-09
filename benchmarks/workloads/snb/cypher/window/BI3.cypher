// BI3, window setting (rule W1): one co-valid binding of the place chain, moderator, forum, the REPLY_OF*0.. walk,
// the message and its tag in the class.
MATCH
  (co:Country {name: $country})<-[ip:IS_PART_OF]-(ci:City)<-[il:IS_LOCATED_IN]-
  (person:Person)<-[hm:HAS_MODERATOR]-(forum:Forum)-[cf:CONTAINER_OF]->(post:Post),
  w = (post)<-[:REPLY_OF*0..]-(message:Message),
  (message)-[ht:HAS_TAG]->(t:Tag)-[hy:HAS_TYPE]->(tc:TagClass {name: $tagClass})
WHERE W_OK(co, ip, ci, il, person, hm, forum, cf, post, ht, t, hy, tc | nodes(w) + relationships(w))
RETURN forum.id, forum.title, forum.creationDate, person.id, count(DISTINCT message) AS messageCount
ORDER BY messageCount DESC, forum.id ASC
LIMIT 20
