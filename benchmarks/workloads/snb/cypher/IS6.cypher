// IS6. Deviation: + VIS on every bound variable; the REPLY_OF*0.. walk is a named path checked with VISALL.
MATCH w = (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post), (p)<-[c:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
WHERE VISALL(nodes(w)) AND VISALL(relationships(w)) AND VIS(c, f, hm, mod)
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
