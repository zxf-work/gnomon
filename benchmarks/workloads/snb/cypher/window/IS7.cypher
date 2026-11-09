// IS7, window setting (rule W1, optional reading O1): one row with true per co-valid friendship of the authors, and
// one row with false when some instant of the binding has none (W_UNCOVERED).
MATCH (m:Message {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE W_OK(m, ro, c, hc, p)
WITH m, c, p, W_LOV(m, ro, c, hc, p) AS lo, W_HIV(m, ro, c, hc, p) AS hi
OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
WHERE W_INV(lo, hi, ha, a, r)
WITH c, p, lo, hi, collect(CASE WHEN r IS NULL THEN null ELSE W_EXT(ha, a, r) END) AS ext
UNWIND [x IN ext | true] + CASE WHEN W_UNCOVERED(lo, hi, ext) THEN [false] ELSE [] END AS knows
RETURN c.id AS commentId,
    c.content AS commentContent,
    c.creationDate AS commentCreationDate,
    p.id AS replyAuthorId,
    p.firstName AS replyAuthorFirstName,
    p.lastName AS replyAuthorLastName,
    knows AS replyAuthorKnowsOriginalMessageAuthor
ORDER BY commentCreationDate DESC, replyAuthorId, knows
