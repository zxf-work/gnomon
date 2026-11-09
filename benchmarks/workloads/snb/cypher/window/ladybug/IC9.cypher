// IC9 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC9.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R2, R5.
MATCH (friend:Person)<-[h:HAS_CREATOR]-(message:Post:Comment)
WHERE message.creationDate < $maxDate AND friend.id <> $personId AND W_OK(friend, h, message)
WITH friend, h, message, W_LOV(friend, h, message) AS lo, W_HIV(friend, h, message) AS hi
MATCH w = (root:Person {id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH DISTINCT friend, message
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
