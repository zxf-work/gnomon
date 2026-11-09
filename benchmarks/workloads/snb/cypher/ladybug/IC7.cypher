// HAND IC7 for LadybugDB (instant settings), from cypher/IC7.cypher. Changes (D-W3-13): the latest like per liker is
// head(collect()) after ORDER BY in Neo4j; LadybugDB allows ORDER BY in WITH only with SKIP/LIMIT and keeps no order
// through an aggregation, so per liker (node key) the largest likeTime, then among the likes at that time the
// smallest message id (the Neo4j order's tie-break), each by a re-match; the liker and message columns are carried
// as values (a node carried through grouping WITHs lost its properties, LB-6; a struct field read in a lambda
// compared wrongly, LB-10); the latency in minutes is whole seconds from to_epoch_ms (Neo4j's
// duration.inSeconds(...).seconds / 60), computed before the last aggregation (in the final RETURN LadybugDB raised
// "Cannot evaluate expression with type PROPERTY").
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)<-[like:LIKES]-(liker:Person)
WHERE VIS(person, h, message, like, liker)
WITH liker.gid AS lg, max(like.creationDate) AS likeTime
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)<-[like:LIKES]-(liker:Person)
WHERE liker.gid = lg AND like.creationDate = likeTime AND VIS(person, h, message, like, liker)
WITH lg, likeTime, min(message.id) AS m0
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)<-[like:LIKES]-(liker:Person)
WHERE liker.gid = lg AND like.creationDate = likeTime AND message.id = m0 AND VIS(person, h, message, like, liker)
WITH DISTINCT lg, liker.id AS lid, liker.firstName AS lfn, liker.lastName AS lln, likeTime, message.id AS mid,
     coalesce(message.imageFile, message.content) AS mc,
     ((to_epoch_ms(likeTime) - to_epoch_ms(message.creationDate)) / 1000) / 60 AS lat
MATCH (lk:Person)
WHERE lk.gid = lg
OPTIONAL MATCH (lk)-[k:KNOWS]-(p2:Person {id: $personId})
WHERE VIS(k, p2)
WITH lid, lfn, lln, likeTime, mid, mc, lat, count(k) AS nk
RETURN
    lid AS personId,
    lfn AS personFirstName,
    lln AS personLastName,
    likeTime AS likeCreationDate,
    mid AS commentOrPostId,
    mc AS commentOrPostContent,
    lat AS minutesLatency,
    CASE WHEN nk = 0 THEN 1 ELSE 0 END AS isNew
ORDER BY likeCreationDate DESC, personId ASC
LIMIT 20
