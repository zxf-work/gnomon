// HAND IS2 for LadybugDB (instant settings), from cypher/IS2.cypher. Changes (D-W3-13): two rounds. LadybugDB starts a
// recursive pattern from every node of its label unless the source is filtered in the same MATCH (job 49723088:
// 71 s at SF0.1 from the 10 bound messages, against 0.08 s from a filtered source), so the 10 latest messages are
// the first statement's result and the second statement's source list ($gids, node keys); the walk as R5.
MATCH (pp:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)
WHERE VIS(pp, h, message)
RETURN message.gid AS g, message.id AS messageId, message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
// --- ARG gids
MATCH w = (message:Post:Comment)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(post:Post),
      (post)-[h2:HAS_CREATOR]->(person:Person)
WHERE message.gid IN $gids AND VIS(message, post) AND VIS(h2, person)
RETURN
 message.id AS messageId,
 coalesce(message.imageFile, message.content) AS messageContent,
 message.creationDate AS messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
