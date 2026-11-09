// HAND BI13 for LadybugDB (instant settings), from cypher/BI13.cypher by the rules R1-R6 and: the zombie list holds node
// keys (gid), matched back to their nodes after UNWIND (LB-5: a node unwound from a collected list and carried through
// a grouping WITH is not correlated in a later OPTIONAL MATCH, which then counts every match), and the membership test
// is on the key (D-W3-13).
MATCH (country:Country {name: $country})<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(zombie:Person)
WHERE zombie.creationDate < $endDate AND VIS(country, ip, c, il, zombie)
WITH country, zombie
OPTIONAL MATCH (zombie)<-[hc:HAS_CREATOR]-(message:Post:Comment)
WHERE message.creationDate < $endDate AND VIS(hc, message)
WITH
  country,
  zombie,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (date_part('year', $endDate)  - date_part('year', zombie.creationDate) )
     + (date_part('month', $endDate) - date_part('month', zombie.creationDate))
     + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie.gid) AS zombies
UNWIND zombies AS zg
MATCH (zombie:Person)
WHERE zombie.gid = zg
OPTIONAL MATCH
  (zombie)<-[h1:HAS_CREATOR]-(message:Post:Comment)<-[l1:LIKES]-(likerZombie:Person)
WHERE likerZombie.gid IN zombies AND VIS(h1, message, l1, likerZombie)
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[h2:HAS_CREATOR]-(message:Post:Comment)<-[l2:LIKES]-(likerPerson:Person)
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
    ELSE zombieLikeCount / CAST(totalLikeCount AS DOUBLE)
  END AS zombieScore
ORDER BY
  zombieScore DESC,
  zombie.id ASC
LIMIT 100
