// HAND BI8 for LadybugDB (instant settings), from cypher/BI8.cypher. Changes (D-W3-13): the person lists hold node keys
// (gid): LadybugDB cannot use an unwound node value as a pattern node ("Cannot bind person as node pattern"), so the
// unwound key is matched back to its node; the friends' sum is sum(CASE WHEN friend IS NULL THEN 0 ELSE 100 * fi +
// nm2 END) over the per-friend rows, the CASE inside the aggregate (LadybugDB refuses an aggregate over a projection
// of an earlier aggregate as "nested aggregation", LB-7); ORDER BY score + friendsScore uses the returned columns.
MATCH (tag:Tag {name: $tag})
WHERE VIS(tag)
OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
WHERE VIS(interest, person)
WITH tag, collect(person.gid) AS interestedPersons
OPTIONAL MATCH (tag)<-[ht:HAS_TAG]-(message:Post:Comment)-[hc:HAS_CREATOR]->(person:Person)
WHERE $startDate < message.creationDate AND message.creationDate < $endDate AND VIS(ht, message, hc, person)
WITH tag, interestedPersons, collect(person.gid) AS messagePersons
WITH tag, list_concat(coalesce(interestedPersons, CAST([] AS INT64[])), coalesce(messagePersons, CAST([] AS INT64[]))) AS persons
UNWIND persons AS pg
WITH DISTINCT tag, pg
MATCH (person:Person)
WHERE person.gid = pg
OPTIONAL MATCH (tag)<-[i1:HAS_INTEREST]-(person)
WHERE VIS(i1)
WITH tag, person, count(i1) AS ni
OPTIONAL MATCH (tag)<-[t1:HAS_TAG]-(m1:Post:Comment)-[c1:HAS_CREATOR]->(person)
WHERE $startDate < m1.creationDate AND m1.creationDate < $endDate AND VIS(t1, m1, c1)
WITH tag, person, ni, count(m1) AS nm
WITH tag, person, 100 * ni + nm AS score
OPTIONAL MATCH (person)-[k:KNOWS]-(friend:Person)
WHERE VIS(k, friend)
OPTIONAL MATCH (tag)<-[i2:HAS_INTEREST]-(friend)
WHERE VIS(i2)
WITH tag, person, score, friend, count(i2) AS fi
OPTIONAL MATCH (tag)<-[t2:HAS_TAG]-(m2:Post:Comment)-[c2:HAS_CREATOR]->(friend)
WHERE $startDate < m2.creationDate AND m2.creationDate < $endDate AND VIS(t2, m2, c2)
WITH person, score, friend, fi, count(m2) AS nm2
WITH person, score, sum(CASE WHEN friend IS NULL THEN 0 ELSE 100 * fi + nm2 END) AS friendsScore
RETURN person.id AS `person.id`, score, friendsScore
ORDER BY score + friendsScore DESC, `person.id` ASC
LIMIT 100
