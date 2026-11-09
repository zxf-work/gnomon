// BI8. Deviation: + VIS on every variable; the pattern comprehensions are OPTIONAL MATCH counts with VIS (one
// form for Neo4j and Memgraph). The friends' sum is a WITH before RETURN, so the ORDER BY on score + friendsScore
// is not an implicit grouping key (Neo4j 5 refuses that form); likewise
// `100 * ni + count(m1)` is split so no aggregate mixes with a grouping key.
MATCH (tag:Tag {name: $tag})
WHERE VIS(tag)
OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
WHERE VIS(interest, person)
WITH tag, collect(person) AS interestedPersons
OPTIONAL MATCH (tag)<-[ht:HAS_TAG]-(message:Message)-[hc:HAS_CREATOR]->(person:Person)
         WHERE $startDate < message.creationDate
           AND message.creationDate < $endDate AND VIS(ht, message, hc, person)
WITH tag, interestedPersons, interestedPersons + collect(person) AS persons
UNWIND persons AS person
WITH DISTINCT tag, person
OPTIONAL MATCH (tag)<-[i1:HAS_INTEREST]-(person)
WHERE VIS(i1)
WITH tag, person, count(i1) AS ni
OPTIONAL MATCH (tag)<-[t1:HAS_TAG]-(m1:Message)-[c1:HAS_CREATOR]->(person)
WHERE $startDate < m1.creationDate AND m1.creationDate < $endDate AND VIS(t1, m1, c1)
WITH tag, person, ni, count(m1) AS nm
WITH tag, person, 100 * ni + nm AS score
OPTIONAL MATCH (person)-[k:KNOWS]-(friend)
WHERE VIS(k, friend)
OPTIONAL MATCH (tag)<-[i2:HAS_INTEREST]-(friend)
WHERE VIS(i2)
WITH tag, person, score, friend, count(i2) AS fi
OPTIONAL MATCH (tag)<-[t2:HAS_TAG]-(m2:Message)-[c2:HAS_CREATOR]->(friend)
WHERE $startDate < m2.creationDate AND m2.creationDate < $endDate AND VIS(t2, m2, c2)
WITH person, score, friend, fi, count(m2) AS nm2
WITH person, score, friend, 100 * fi + nm2 AS friendScore
WITH person, score, sum(CASE WHEN friend IS NULL THEN 0 ELSE friendScore END) AS friendsScore
RETURN
  person.id,
  score,
  friendsScore
ORDER BY
  score + friendsScore DESC,
  person.id ASC
LIMIT 100
