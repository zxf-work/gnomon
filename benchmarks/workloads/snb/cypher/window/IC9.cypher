// IC9, window setting (rule W1): a friend qualifies for a (friend, message) binding when some KNOWS*1..2 walk from
// the start person is co-valid with it (W_INV over the walk's elements).
MATCH (friend:Person)<-[h:HAS_CREATOR]-(message:Message)
WHERE message.creationDate < $maxDate AND friend.id <> $personId AND W_OK(friend, h, message)
WITH friend, h, message, W_LOV(friend, h, message) AS lo, W_HIV(friend, h, message) AS hi
MATCH w = (root:Person {id: $personId })-[:KNOWS*1..2]-(friend)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
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
