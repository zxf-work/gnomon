// BI5, window setting (rule W1): (tag, tag edge, message, creator edge, person) bindings; the like and reply counts are
// stages over their own valid bindings.
MATCH (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Message)-[hc:HAS_CREATOR]->(person:Person)
WHERE W_OK(tag, ht, message, hc, person)
OPTIONAL MATCH (message)<-[likes:LIKES]-(liker:Person)
WHERE W_OK(likes, liker)
WITH person, message, count(likes) AS likeCount
OPTIONAL MATCH (message)<-[ro:REPLY_OF]-(reply:Comment)
WHERE W_OK(ro, reply)
WITH person, message, likeCount, count(reply) AS replyCount
WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
RETURN person.id, replyCount, likeCount, messageCount, 1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY score DESC, person.id ASC
LIMIT 100
