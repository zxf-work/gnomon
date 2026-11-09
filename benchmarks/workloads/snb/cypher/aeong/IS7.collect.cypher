// T-LDBC IS7 for AeonG, P17 test form (b): the knows flag as membership in the original author's visible friends.
// Each OPTIONAL MATCH starts at a bound node and ends at a new one (the form P17 does not affect); the two directed
// steps replace the undirected one (D-W3-10). At a given (T, V) a person has one visible version, so comparing ids
// is the same test as the LDBC pattern.
MATCH (m:Message {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE VISTT(m, ro, c, hc, p) TT AS $AT
    OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)
    WHERE VISTT(ha, a) TT AS $AT
    OPTIONAL MATCH (a)-[r1:KNOWS]->(f1)
    WHERE VISTT(r1, f1) TT AS $AT
    WITH ro, c, hc, p, a, collect(f1.id) AS out1
    OPTIONAL MATCH (a)<-[r2:KNOWS]-(f2)
    WHERE VISTT(r2, f2) TT AS $AT
    WITH ro, c, hc, p, out1 + collect(f2.id) AS fids
    RETURN c.id AS commentId,
        c.content AS commentContent,
        c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        p.id IN fids AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId
