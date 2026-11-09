// IC5, window setting (rule W1): the (forum, membership) binding with a co-valid friend walk (1..2 hops), carried with
// its own interval; a post (the optional part) counts when it shares an instant with them (G11 verdict 1).
MATCH (forum:Forum)-[membership:HAS_MEMBER]->(otherPerson:Person)
WHERE membership.creationDate > $minDate AND otherPerson.id <> $personId AND W_OK(forum, membership)
WITH forum, membership, otherPerson, W_LOV(forum, membership) AS lo, W_HIV(forum, membership) AS hi
MATCH w = (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
WHERE W_INV(lo, hi, | nodes(w) + relationships(w))
WITH forum, membership, otherPerson, collect(DISTINCT {lo: W_LOV(forum, membership | nodes(w) + relationships(w)), hi: W_HIV(forum, membership | nodes(w) + relationships(w))}) AS walks
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
