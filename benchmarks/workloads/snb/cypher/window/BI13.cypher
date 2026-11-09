// BI13, window setting (rule W1): zombies are a stage over (country, place chain, person) bindings with their
// co-valid messages; the like counts are a stage over valid (creator edge, message, like, liker) bindings.
MATCH (country:Country {name: $country})<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(zombie:Person)
WHERE zombie.creationDate < $endDate AND W_OK(country, ip, c, il, zombie)
WITH zombie, W_LOV(country, ip, c, il, zombie) AS lo, W_HIV(country, ip, c, il, zombie) AS hi
OPTIONAL MATCH (zombie)<-[hc:HAS_CREATOR]-(message:Message)
WHERE message.creationDate < $endDate AND message.creationDate >= zombie.creationDate AND W_INV(lo, hi, hc, message)
WITH zombie, count(message) AS messageCount
WITH zombie, 12 * ($endDate.year - zombie.creationDate.year) + ($endDate.month - zombie.creationDate.month) + 1 AS months, messageCount
WHERE messageCount < months
WITH collect(DISTINCT zombie.id) AS zombies
UNWIND zombies AS zid
OPTIONAL MATCH (:Person {id: zid})<-[h1:HAS_CREATOR]-(message:Message)<-[l1:LIKES]-(liker:Person)
WHERE liker.creationDate < $endDate AND W_OK(h1, message, l1, liker)
WITH zid, zombies, count(liker) AS totalLikeCount, sum(CASE WHEN liker.id IN zombies THEN 1 ELSE 0 END) AS zombieLikeCount
RETURN zid AS `zombie.id`, zombieLikeCount, totalLikeCount,
  CASE totalLikeCount WHEN 0 THEN 0.0 ELSE zombieLikeCount / toFloat(totalLikeCount) END AS zombieScore
ORDER BY zombieScore DESC, `zombie.id` ASC
LIMIT 100
