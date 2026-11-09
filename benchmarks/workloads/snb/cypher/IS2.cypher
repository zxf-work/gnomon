// IS2. Deviation: + VIS on every bound variable; the REPLY_OF*0.. walk is a named path checked with VISALL.
MATCH (pp:Person {id: $personId})<-[h:HAS_CREATOR]-(message)
WHERE VIS(pp, h, message)
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH w = (message)-[:REPLY_OF*0..]->(post:Post),
      (post)-[h2:HAS_CREATOR]->(person)
WHERE VISALL(nodes(w)) AND VISALL(relationships(w)) AND VIS(h2, person)
RETURN
 messageId,
 coalesce(message.imageFile,message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
