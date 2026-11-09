// IC9 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC9.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH w = (root:Person {id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE VIS(_r, _n))]-(friend:Person)
WHERE NOT friend = root AND VIS(root, friend)
WITH collect(distinct friend) as friends
UNWIND friends as friend
    MATCH (friend)<-[h:HAS_CREATOR]-(message:Post:Comment)
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
