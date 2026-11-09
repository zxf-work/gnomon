// HAND BI8 for LadybugDB (window, rule W1), from cypher/window/BI8.cypher. Changes (D-W3-13): no CALL {}: two
// statements. The first returns [person id, score] per person: the persons of the tag (interest or a message in the
// period, each over its own co-valid bindings), each scored by two OPTIONAL MATCH counts per person id (a node
// pattern on a row value is a predicate, q.id = pid). The second (ARG sc) adds the friends' score: per person its
// friend ids (collect over a sentinel -1, so no person is lost: LB-8), one row per friend id against every scored
// person, summed in one aggregate (LB-7 refuses a sum over an earlier aggregate's projection); a friend id listed
// twice counts twice, as in Neo4j's reduce(). The two-hop OPTIONAL MATCHes whose test reads relationships keep only
// their node predicates in WHERE and read the co-valid test as a flag in the count (LB-3 in its OPTIONAL MATCH form;
// a failing row adds nothing, as in Neo4j's OPTIONAL MATCH ... WHERE). No lambda reads an outer variable (LB-10).
MATCH (tag:Tag {name: $tag})
OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
WHERE W_OK(tag, interest, person)
WITH tag, collect(person.id) AS interestedPersons
OPTIONAL MATCH (tag)<-[ht:HAS_TAG]-(message:Post:Comment)-[hc:HAS_CREATOR]->(person:Person)
WHERE $startDate < message.creationDate AND message.creationDate < $endDate
WITH tag, interestedPersons, person, W_OK(tag, ht, message, hc, person) AS ok
WITH tag, interestedPersons, collect(CASE WHEN ok THEN person.id ELSE NULL END) AS messagePersons
WITH tag, list_concat(coalesce(interestedPersons, CAST([] AS INT64[])), coalesce(messagePersons, CAST([] AS INT64[]))) AS persons
UNWIND persons AS pid
WITH DISTINCT tag, pid
OPTIONAL MATCH (tag)<-[i1:HAS_INTEREST]-(p:Person)
WHERE p.id = pid AND W_OK(tag, i1, p)
WITH tag, pid, count(DISTINCT p) AS ni
OPTIONAL MATCH (tag)<-[t1:HAS_TAG]-(m1:Post:Comment)-[c1:HAS_CREATOR]->(q:Person)
WHERE q.id = pid AND $startDate < m1.creationDate AND m1.creationDate < $endDate
WITH pid, ni, m1, W_OK(tag, t1, m1, c1, q) AS ok
WITH pid, ni, count(CASE WHEN ok THEN m1.gid ELSE NULL END) AS nm
RETURN [pid, CASE WHEN ni > 0 THEN 100 ELSE 0 END + nm] AS ps
// --- ARG sc
UNWIND $sc AS s
WITH s[1] AS pid, s[2] AS score
OPTIONAL MATCH (q:Person)-[k:KNOWS]-(f:Person)
WHERE q.id = pid AND W_OK(k)
WITH pid, score, collect(CASE WHEN f IS NULL THEN CAST(-1 AS INT64) ELSE f.id END) AS friends
UNWIND friends AS fid
UNWIND $sc AS s2
WITH pid, score, sum(CASE WHEN s2[1] = fid THEN s2[2] ELSE 0 END) AS friendsScore
RETURN pid AS `person.id`, score, friendsScore
ORDER BY score + friendsScore DESC, `person.id` ASC
LIMIT 100
