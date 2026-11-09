// IC5. Deviation: + VIS on every variable, the KNOWS*1..2 walk as a named path (VISALL).
MATCH w = (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
WHERE
    person <> otherPerson AND VISALL(nodes(w)) AND VISALL(relationships(w))
WITH DISTINCT otherPerson
MATCH (otherPerson)<-[membership:HAS_MEMBER]-(forum)
WHERE
    membership.creationDate > $minDate AND VIS(membership, forum)
WITH
    forum,
    collect(otherPerson) AS otherPersons
OPTIONAL MATCH (otherPerson2)<-[h:HAS_CREATOR]-(post)<-[c:CONTAINER_OF]-(forum)
WHERE
    otherPerson2 IN otherPersons AND VIS(h, post, c)
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
