// IC2 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC2.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2.
MATCH (p0:Person {id: $personId })-[k:KNOWS]-(friend:Person)<-[h:HAS_CREATOR]-(message:Post:Comment)
    WHERE message.creationDate < $maxDate AND W_OK(p0, k, friend, h, message)
    RETURN
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        message.id AS postOrCommentId,
        coalesce(message.imageFile, message.content) AS postOrCommentContent,
        message.creationDate AS postOrCommentCreationDate
    ORDER BY
        postOrCommentCreationDate DESC,
        CAST(postOrCommentId AS INT64) ASC
    LIMIT 20
