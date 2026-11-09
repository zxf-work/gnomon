// HAND BI16 for LadybugDB (instant settings), from cypher/BI16.cypher. Changes (D-W3-13): LadybugDB has no CALL {}
// subquery: the per-parameter subquery is the same MATCH / OPTIONAL MATCH grouped by (parameter letter, person1),
// which is the subquery's grouping per outer row; date(x) -> CAST(x AS DATE); the tag is matched on its name by a
// predicate (a variable in a property map is not accepted); the two letters' counts are
// conditional sums over the (letter, person) rows, a person kept when it has both (a lambda over the collected
// structs picked the wrong entry, LB-10).
UNWIND [{letter: 'A', tag: $tagA, date: $dateA}, {letter: 'B', tag: $tagB, date: $dateB}] AS param
WITH param.letter AS paramLetter, param.tag AS paramTagX, CAST(param.date AS DATE) AS paramDateX
MATCH (person1:Person)<-[h1:HAS_CREATOR]-(message1:Post:Comment)-[t1:HAS_TAG]->(tag:Tag)
WHERE tag.name = paramTagX AND CAST(message1.creationDate AS DATE) = paramDateX AND VIS(person1, h1, message1, t1, tag)
OPTIONAL MATCH (person1)-[k:KNOWS]-(person2:Person)<-[h2:HAS_CREATOR]-(message2:Post:Comment)-[t2:HAS_TAG]->(tag)
WHERE CAST(message2.creationDate AS DATE) = paramDateX AND VIS(k, person2, h2, message2, t2)
WITH paramLetter, person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
WHERE cp2 <= $maxKnowsLimit
WITH person1, count(*) AS nres, sum(CASE WHEN paramLetter = 'A' THEN cm ELSE 0 END) AS messageCountA,
     sum(CASE WHEN paramLetter = 'B' THEN cm ELSE 0 END) AS messageCountB
WHERE nres = 2
RETURN person1.id AS `person1.id`, messageCountA, messageCountB
ORDER BY messageCountA + messageCountB DESC, `person1.id` ASC
LIMIT 20
