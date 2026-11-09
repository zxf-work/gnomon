// BI9, window setting (rule W1): the (person, creator edge, post) binding; the per-thread message count is a stage
// over valid walks from the post.
MATCH (person:Person)<-[hc:HAS_CREATOR]-(post:Post)
WHERE post.creationDate >= $startDate AND post.creationDate <= $endDate AND W_OK(person, hc, post)
WITH DISTINCT person, post
OPTIONAL MATCH w = (post)<-[:REPLY_OF*0..]-(reply:Message)
WHERE reply.creationDate >= $startDate AND reply.creationDate <= $endDate AND W_OK(| nodes(w) + relationships(w))
WITH person, post, count(DISTINCT reply) AS msgs
RETURN person.id, person.firstName, person.lastName, count(DISTINCT post) AS threadCount, sum(msgs) AS messageCount
ORDER BY messageCount DESC, person.id ASC
LIMIT 100
