// IS3 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IS3.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2.
MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
WHERE VIS(n, r, friend)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    CAST(personId AS INT64) ASC
