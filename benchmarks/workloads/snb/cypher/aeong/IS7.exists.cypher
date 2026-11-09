// T-LDBC IS7 for AeonG, P17 test form (a): the knows flag as a pattern predicate. The predicate cannot carry the
// relationship's own visibility (VISTT on r), so it is exact only where no KNOWS copy between two visible person
// versions has been closed; it is tested here, not adopted.
MATCH (m:Message {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE VISTT(m, ro, c, hc, p) TT AS $AT
    OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)
    WHERE VISTT(ha, a) TT AS $AT
    RETURN c.id AS commentId,
        c.content AS commentContent,
        c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        CASE
            WHEN a IS NULL THEN false
            WHEN exists((a)-[:KNOWS]->(p)) OR exists((a)<-[:KNOWS]-(p)) THEN true
            ELSE false
        END AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId
