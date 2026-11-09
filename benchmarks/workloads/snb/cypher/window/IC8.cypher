// IC8, window setting (rule W1).
MATCH (start:Person {id: $personId})<-[h1:HAS_CREATOR]-(m:Message)<-[r:REPLY_OF]-(comment:Comment)-[h2:HAS_CREATOR]->(person:Person)
WHERE W_OK(start, h1, m, r, comment, h2, person)
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
