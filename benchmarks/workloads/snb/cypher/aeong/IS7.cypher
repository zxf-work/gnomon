// T-LDBC IS7 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
// AeonG-only workaround (D-W3-11, P17): the replies are collected first, so each OPTIONAL MATCH runs on one input
// row (under TT AS, AeonG's OPTIONAL MATCH on several input rows matches nothing and nulls bound variables), then
// unwound. The knows flag is membership in the original author's visible friends, read in both directions
// (D-W3-10). At a given (T, V) a person has one visible version, so comparing ids is the same test as the pattern.
MATCH (m:Message {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE VISTT(m, ro, c, hc, p) TT AS $AT
WITH m, collect({c: c, p: p}) AS replies
    OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)
    WHERE VISTT(ha, a) TT AS $AT
    WITH replies, a
    OPTIONAL MATCH (a)-[r1:KNOWS]->(f1)
    WHERE VISTT(r1, f1) TT AS $AT
    WITH replies, a, collect(f1.id) AS out1
    OPTIONAL MATCH (a)<-[r2:KNOWS]-(f2)
    WHERE VISTT(r2, f2) TT AS $AT
    WITH replies, out1 + collect(f2.id) AS fids
    UNWIND replies AS rp
    WITH rp.c AS c, rp.p AS p, fids
    RETURN c.id AS commentId,
        c.content AS commentContent,
        c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        p.id IN fids AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId
