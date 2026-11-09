// IC7, window setting (rule W1, optional reading O1): the latest like per liker among co-valid (person, creator edge,
// message, like, liker) bindings; isNew is 0 per co-valid friendship and 1 when some instant has none.
MATCH (person:Person {id: $personId})<-[h:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
    WHERE W_OK(person, h, message, like, liker)
    WITH liker.id AS lid, liker, message, like.creationDate AS likeTime, W_LOV(person, h, message, like, liker) AS lo, W_HIV(person, h, message, like, liker) AS hi
    ORDER BY likeTime DESC, toInteger(message.id) ASC
    // per liker (by id: a liker has one node per version), the latest like; the 20 latest likers
    WITH lid, head(collect({liker: liker, msg: message, likeTime: likeTime, lo: lo, hi: hi})) AS latestLike
    ORDER BY latestLike.likeTime DESC, toInteger(lid) ASC
    LIMIT 20
WITH latestLike.liker AS liker, latestLike, latestLike.lo AS lo, latestLike.hi AS hi
OPTIONAL MATCH (liker)-[k:KNOWS]-(p2:Person {id: $personId})
WHERE W_INV(lo, hi, k, p2)
WITH liker, latestLike, lo, hi, collect(CASE WHEN k IS NULL THEN null ELSE W_EXT(k, p2) END) AS ext
UNWIND CASE WHEN size(ext) > 0 THEN [0] ELSE [] END + CASE WHEN W_UNCOVERED(lo, hi, ext) THEN [1] ELSE [] END AS isNew
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.id AS commentOrPostId,
    coalesce(latestLike.msg.imageFile, latestLike.msg.content) AS commentOrPostContent,
    duration.inSeconds(latestLike.msg.creationDate, latestLike.likeTime).seconds / 60 AS minutesLatency,
    isNew
ORDER BY likeCreationDate DESC, toInteger(personId) ASC, isNew
