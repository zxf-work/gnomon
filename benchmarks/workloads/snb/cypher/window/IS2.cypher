// IS2, window setting (rule W1): the last 10 messages among co-valid (person, message) bindings; the REPLY_OF*0..
// walk to the root and its author must be co-valid with that binding (W_INV).
MATCH (pp:Person {id: $personId})<-[h:HAS_CREATOR]-(message)
WHERE W_OK(pp, h, message)
WITH
 message, W_LOV(pp, h, message) AS lo, W_HIV(pp, h, message) AS hi,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH w = (message)-[:REPLY_OF*0..]->(post:Post),
      (post)-[h2:HAS_CREATOR]->(person)
WHERE W_INV(lo, hi, h2, person | nodes(w) + relationships(w))
RETURN
 messageId,
 coalesce(message.imageFile,message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
