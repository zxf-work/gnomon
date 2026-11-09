// IC8 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC8.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R6 (h1.rid <> h2.rid).
MATCH (start:Person {id: $personId})<-[h1:HAS_CREATOR]-(m:Post:Comment)<-[r:REPLY_OF]-(comment:Comment)-[h2:HAS_CREATOR]->(person:Person)
WHERE h1.rid <> h2.rid AND VIS(start, h1, m, r, comment, h2, person)
RETURN
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName,
    comment.creationDate AS commentCreationDate,
    comment.id AS commentId,
    comment.content AS commentContent
ORDER BY
    commentCreationDate DESC,
    commentId ASC
LIMIT 20
