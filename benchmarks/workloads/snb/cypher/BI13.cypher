// BI13. Deviation: + VIS on every variable.
MATCH (country:Country {name: $country})<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(zombie:Person)
WHERE zombie.creationDate < $endDate AND VIS(country, ip, c, il, zombie)
WITH country, zombie
OPTIONAL MATCH (zombie)<-[hc:HAS_CREATOR]-(message:Message)
WHERE message.creationDate < $endDate AND VIS(hc, message)
WITH
  country,
  zombie,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * ($endDate.year  - zombie.creationDate.year )
     + ($endDate.month - zombie.creationDate.month)
     + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[h1:HAS_CREATOR]-(message:Message)<-[l1:LIKES]-(likerZombie:Person)
WHERE likerZombie IN zombies AND VIS(h1, message, l1, likerZombie)
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[h2:HAS_CREATOR]-(message:Message)<-[l2:LIKES]-(likerPerson:Person)
WHERE likerPerson.creationDate < $endDate AND VIS(h2, message, l2, likerPerson)
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount
RETURN
  zombie.id,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / toFloat(totalLikeCount)
  END AS zombieScore
ORDER BY
  zombieScore DESC,
  zombie.id ASC
LIMIT 100
