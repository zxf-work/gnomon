// IC2, window setting (rule W1).
MATCH (p0:Person {id: $personId })-[k:KNOWS]-(friend:Person)<-[h:HAS_CREATOR]-(message:Message)
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
        toInteger(postOrCommentId) ASC
    LIMIT 20
