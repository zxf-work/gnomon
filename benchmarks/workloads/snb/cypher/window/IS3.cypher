// IS3, window setting (rule W1).
MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
WHERE W_OK(n, r, friend)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    toInteger(personId) ASC
