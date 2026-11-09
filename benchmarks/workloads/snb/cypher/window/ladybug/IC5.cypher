// IC5 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC5.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (forum:Forum)-[membership:HAS_MEMBER]->(otherPerson:Person)
WHERE membership.creationDate > $minDate AND otherPerson.id <> $personId AND W_OK(forum, membership)
WITH forum, membership, otherPerson, W_LOV(forum, membership) AS lo, W_HIV(forum, membership) AS hi
MATCH w = (person:Person { id: $personId })-[:KNOWS* TRAIL 1..2 (_r, _n | WHERE WPRUNE(_r, _n))]-(otherPerson)
WHERE W_INV(lo, hi, | nodes(w) + rels(w))
WITH forum, membership, otherPerson, collect(DISTINCT {lo: W_LOV(forum, membership | nodes(w) + rels(w)), hi: W_HIV(forum, membership | nodes(w) + rels(w))}) AS walks
OPTIONAL MATCH (otherPerson)<-[h:HAS_CREATOR]-(post:Post)<-[c:CONTAINER_OF]-(forum)
WHERE any(wk IN walks WHERE W_INV(wk.lo, wk.hi, h, post, c))
WITH forum, count(DISTINCT post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
