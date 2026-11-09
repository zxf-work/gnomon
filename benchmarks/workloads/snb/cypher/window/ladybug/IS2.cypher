// IS2 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IS2.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (pp:Person {id: $personId})<-[h:HAS_CREATOR]-(message)
WHERE W_OK(pp, h, message)
WITH
 message, W_LOV(pp, h, message) AS lo, W_HIV(pp, h, message) AS hi,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH w = (message)-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE WPRUNE(_r, _n))]->(post:Post),
      (post)-[h2:HAS_CREATOR]->(person)
WHERE W_INV(lo, hi, h2, person | nodes(w) + rels(w))
RETURN
 messageId,
 coalesce(message.imageFile,message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
