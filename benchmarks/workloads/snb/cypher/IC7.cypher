// IC7. Deviation: + VIS on every variable (inside the isNew pattern predicate as an EXISTS subquery); the latency
// uses duration arithmetic on our LocalDateTime dates (LDBC's text subtracts epoch-millisecond integers).
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
    WHERE VIS(person, h, message, like, liker)
    WITH liker, message, like.creationDate AS likeTime, person
    ORDER BY likeTime DESC, toInteger(message.id) ASC
    WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
OPTIONAL MATCH (liker)-[k:KNOWS]-(p2:Person {id: $personId})
WHERE VIS(k, p2)
WITH liker, latestLike, count(k) AS nk
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.id AS commentOrPostId,
    coalesce(latestLike.msg.imageFile, latestLike.msg.content) AS commentOrPostContent,
    duration.inSeconds(latestLike.msg.creationDate, latestLike.likeTime).seconds / 60 AS minutesLatency,
    CASE WHEN nk = 0 THEN 1 ELSE 0 END AS isNew
ORDER BY
    likeCreationDate DESC,
    toInteger(personId) ASC
LIMIT 20
