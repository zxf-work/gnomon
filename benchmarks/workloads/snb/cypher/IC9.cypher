// IC9. Deviation: + VIS on every variable, the KNOWS*1..2 walk as a named path (VISALL).
MATCH w = (root:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
WHERE NOT friend = root AND VISALL(nodes(w)) AND VISALL(relationships(w))
WITH collect(distinct friend) as friends
UNWIND friends as friend
    MATCH (friend)<-[h:HAS_CREATOR]-(message:Message)
    WHERE message.creationDate < $maxDate AND VIS(h, message)
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS commentOrPostId,
    coalesce(message.imageFile, message.content) AS commentOrPostContent,
    message.creationDate AS commentOrPostCreationDate
ORDER BY
    commentOrPostCreationDate DESC,
    message.id ASC
LIMIT 20
