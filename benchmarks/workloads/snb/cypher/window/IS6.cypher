// IS6, window setting (rule W1): the REPLY_OF*0.. walk counts as bound (its elements share the instant).
MATCH w = (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post), (p)<-[c:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
WHERE W_OK(c, f, hm, mod | nodes(w) + relationships(w))
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
