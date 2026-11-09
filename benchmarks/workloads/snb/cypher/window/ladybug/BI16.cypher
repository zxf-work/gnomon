// HAND BI16 for LadybugDB (window, rule W1), from cypher/window/BI16.cypher. Changes (D-W3-13): no CALL {}: the per-tag/day
// person stage grouped by the parameter letter; date(x) -> CAST(x AS DATE); a node pattern on a row value is a
// predicate (q.id = pid); the two letters' counts are
// conditional sums over the (letter, person) rows, a person kept when it has both (a lambda over the collected
// structs picked the wrong entry, LB-10).
UNWIND [{letter: 'A', tag: $tagA, date: $dateA}, {letter: 'B', tag: $tagB, date: $dateB}] AS param
WITH param.letter AS paramLetter, param.tag AS paramTagX, CAST(param.date AS DATE) AS paramDateX
MATCH (person1:Person)<-[h1:HAS_CREATOR]-(message1:Post:Comment)-[t1:HAS_TAG]->(tag:Tag)
WHERE tag.name = paramTagX AND CAST(message1.creationDate AS DATE) = paramDateX AND W_OK(person1, h1, message1, t1, tag)
WITH paramLetter, collect(DISTINCT person1.id) AS sg, collect({p: person1.id, m: message1.id}) AS pm
UNWIND pm AS x
WITH paramLetter, sg, x.p AS pid, count(DISTINCT x.m) AS cm
OPTIONAL MATCH (q:Person)-[k:KNOWS]-(p2:Person)
WHERE q.id = pid AND p2.id IN sg AND W_OK(k)
WITH paramLetter, pid, cm, count(DISTINCT p2.id) AS cp2
WHERE cp2 <= $maxKnowsLimit
WITH pid, count(*) AS nres, sum(CASE WHEN paramLetter = 'A' THEN cm ELSE 0 END) AS messageCountA,
     sum(CASE WHEN paramLetter = 'B' THEN cm ELSE 0 END) AS messageCountB
WHERE nres = 2
RETURN pid AS `person1.id`, messageCountA, messageCountB
ORDER BY messageCountA + messageCountB DESC, `person1.id` ASC
LIMIT 20
