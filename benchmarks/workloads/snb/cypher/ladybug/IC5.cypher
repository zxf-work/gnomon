// IC5 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IC5.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH w = (person:Person { id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE VIS(_r, _n))]-(otherPerson)
WHERE
    person <> otherPerson AND VIS(person, otherPerson)
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
