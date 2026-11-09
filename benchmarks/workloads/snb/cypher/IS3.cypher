// IS3. Deviation: + VIS on every bound variable.
MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
WHERE VIS(n, r, friend)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    toInteger(personId) ASC
