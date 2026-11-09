// IS7. Deviation: + VIS on every bound variable, inside the OPTIONAL MATCH as well.
MATCH (m:Message {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
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
