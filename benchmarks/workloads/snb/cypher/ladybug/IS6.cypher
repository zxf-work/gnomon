// IS6 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IS6.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH w = (m:Post:Comment {id: $messageId })-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE VIS(_r, _n))]->(p:Post), (p)<-[c:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
WHERE VIS(m, p) AND VIS(c, f, hm, mod)
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
