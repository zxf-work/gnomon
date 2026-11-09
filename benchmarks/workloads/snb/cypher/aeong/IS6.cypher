// T-LDBC IS6 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
MATCH w = (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post), (p)<-[c:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
WHERE VISTTALL(nodes(w)) AND VISTTALL(relationships(w)) AND VISTT(c, f, hm, mod) TT AS $AT
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
