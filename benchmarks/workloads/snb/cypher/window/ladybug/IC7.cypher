// HAND IC7 for LadybugDB (window, rule W1 and O1), from cypher/window/IC7.cypher. Changes (D-W3-13): the latest like
// per liker id is head(collect()) after ORDER BY in Neo4j (no order through an aggregation in LadybugDB): the
// largest likeTime per liker id and the 20 latest likers, then, as the SQL reference does, every co-valid binding
// (person, creator edge, message, like, liker versions) of that liker and like time, each with its own [lo, hi) and
// its own isNew rows (O1); the latency as in the instant text; [0] / [1] lists as Neo4j's. (An earlier form kept
// one binding per liker, the smallest message id, liker key and lo, and missed a second binding's isNew row:
// IC7 window 1 of 3 at SF0.1, 2026-10-24.)
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)<-[like:LIKES]-(liker:Person)
WHERE W_OK(person, h, message, like, liker)
WITH liker.id AS lid, max(like.creationDate) AS likeTime
ORDER BY likeTime DESC, lid ASC
LIMIT 20
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Post:Comment)<-[like:LIKES]-(liker:Person)
WHERE liker.id = lid AND like.creationDate = likeTime AND W_OK(person, h, message, like, liker)
WITH liker, message, like, person, h, likeTime, W_LOV(person, h, message, like, liker) AS lo, W_HIV(person, h, message, like, liker) AS hi
OPTIONAL MATCH (liker)-[k:KNOWS]-(p2:Person {id: $personId})
WHERE W_INV(lo, hi, k, p2)
WITH liker, message, like, person, h, likeTime, lo, hi, collect(CASE WHEN k IS NULL THEN NULL ELSE W_EXT(k, p2) END) AS ext
UNWIND list_concat(CASE WHEN size(ext) > 0 THEN [0] ELSE CAST([] AS INT64[]) END, CASE WHEN W_UNCOVERED(lo, hi, ext) THEN [1] ELSE CAST([] AS INT64[]) END) AS isNew
WITH liker.id AS lid, liker.firstName AS lfn, liker.lastName AS lln, likeTime, message.id AS mid2, coalesce(message.imageFile, message.content) AS mcontent,
     ((to_epoch_ms(likeTime) - to_epoch_ms(message.creationDate)) / 1000) / 60 AS lat, isNew
RETURN
    lid AS personId,
    lfn AS personFirstName,
    lln AS personLastName,
    likeTime AS likeCreationDate,
    mid2 AS commentOrPostId,
    mcontent AS commentOrPostContent,
    lat AS minutesLatency,
    isNew
ORDER BY likeCreationDate DESC, personId ASC, isNew
