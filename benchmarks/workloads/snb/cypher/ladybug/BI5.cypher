// BI5 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI5.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1.
MATCH (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Post:Comment)-[hc:HAS_CREATOR]->(person:Person)
WHERE VIS(tag, ht, message, hc, person)
OPTIONAL MATCH (message)<-[likes:LIKES]-(liker:Person)
WHERE VIS(likes, liker)
WITH person, message, count(likes) AS likeCount
OPTIONAL MATCH (message)<-[ro:REPLY_OF]-(reply:Comment)
WHERE VIS(ro, reply)
WITH person, message, likeCount, count(reply) AS replyCount
WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
RETURN
  person.id,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY
  score DESC,
  person.id ASC
LIMIT 100
