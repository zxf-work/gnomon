// IS7 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IS7.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1.
MATCH (m:Post:Comment {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE VIS(m, ro, c, hc, p)
    OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
    WHERE VIS(ha, a, r)
    RETURN c.id AS commentId,
        c.content AS commentContent,
        c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        CASE
            WHEN r IS NULL THEN false
            ELSE true
        END AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId
