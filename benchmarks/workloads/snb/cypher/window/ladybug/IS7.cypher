// HAND IS7 for LadybugDB (window, rule W1 and O1), from cypher/window/IS7.cypher by R1-R6 and: an empty collect() is NULL
// in LadybugDB (LB-8), so the extension list is coalesced to the empty list; list concatenation is list_concat.
MATCH (m:Post:Comment {id: $messageId })<-[ro:REPLY_OF]-(c:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE W_OK(m, ro, c, hc, p)
WITH m, c, p, W_LOV(m, ro, c, hc, p) AS lo, W_HIV(m, ro, c, hc, p) AS hi
OPTIONAL MATCH (m)-[ha:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
WHERE W_INV(lo, hi, ha, a, r)
WITH c, p, lo, hi, collect(CASE WHEN r IS NULL THEN null ELSE W_EXT(ha, a, r) END) AS ext
UNWIND list_concat(list_transform(coalesce(ext, CAST([] AS INT64[][])), x -> true), CASE WHEN W_UNCOVERED(lo, hi, ext) THEN [false] ELSE CAST([] AS BOOLEAN[]) END) AS knows
RETURN c.id AS commentId,
    c.content AS commentContent,
    c.creationDate AS commentCreationDate,
    p.id AS replyAuthorId,
    p.firstName AS replyAuthorFirstName,
    p.lastName AS replyAuthorLastName,
    knows AS replyAuthorKnowsOriginalMessageAuthor
ORDER BY commentCreationDate DESC, replyAuthorId, knows
