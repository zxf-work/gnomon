// T-LDBC IS2 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
MATCH (pp:Person {id: $personId})<-[h:HAS_CREATOR]-(message)
WHERE VISTT(pp, h, message) TT AS $AT
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH w = (message)-[:REPLY_OF*0..]->(post:Post),
      (post)-[h2:HAS_CREATOR]->(person)
WHERE VISTTALL(nodes(w)) AND VISTTALL(relationships(w)) AND VISTT(h2, person) TT AS $AT
RETURN
 messageId,
 coalesce(message.imageFile,message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
