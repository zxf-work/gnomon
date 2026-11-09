// HAND BI13 for LadybugDB (window), from window/BI13.cypher by the rules R1, R2 and: the likes' OPTIONAL MATCH
// (two hops, its WHERE reads the relationships h1 and l1) runs without a WHERE, and the test is a flag the counts
// read (count(CASE WHEN ok ...), sum(CASE WHEN ok AND ...)): the same counts, since a row that fails the test adds
// nothing, as in Neo4j's OPTIONAL MATCH ... WHERE (LB-3 in its OPTIONAL MATCH form: the filtered two-hop OPTIONAL
// MATCH lost likes; degenerate-window check against the asof answers, 2026-10-24).
MATCH (country:Country {name: $country})<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(zombie:Person)
WHERE zombie.creationDate < $endDate AND W_OK(country, ip, c, il, zombie)
WITH zombie, W_LOV(country, ip, c, il, zombie) AS lo, W_HIV(country, ip, c, il, zombie) AS hi
OPTIONAL MATCH (zombie)<-[hc:HAS_CREATOR]-(message:Post:Comment)
WHERE message.creationDate < $endDate AND message.creationDate >= zombie.creationDate AND W_INV(lo, hi, hc, message)
WITH zombie, count(message) AS messageCount
WITH zombie, 12 * (date_part('year', $endDate) - date_part('year', zombie.creationDate)) + (date_part('month', $endDate) - date_part('month', zombie.creationDate)) + 1 AS months, messageCount
WHERE messageCount < months
WITH collect(DISTINCT zombie.id) AS zombies
UNWIND zombies AS zid
OPTIONAL MATCH (:Person {id: zid})<-[h1:HAS_CREATOR]-(message:Post:Comment)<-[l1:LIKES]-(liker:Person)
WITH zid, zombies, liker, (liker.creationDate < $endDate AND W_OK(h1, message, l1, liker)) AS ok
WITH zid, zombies, count(CASE WHEN ok THEN liker.gid ELSE NULL END) AS totalLikeCount, sum(CASE WHEN ok AND liker.id IN zombies THEN 1 ELSE 0 END) AS zombieLikeCount
RETURN zid AS `zombie.id`, zombieLikeCount, totalLikeCount,
  CASE totalLikeCount WHEN 0 THEN 0.0 ELSE zombieLikeCount / CAST(totalLikeCount AS DOUBLE) END AS zombieScore
ORDER BY zombieScore DESC, `zombie.id` ASC
LIMIT 100
