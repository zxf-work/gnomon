// HAND BI4 for LadybugDB (window, rule W1), from cypher/window/BI4.cypher. Changes (D-W3-13): no CALL {}: three
// statements. (1) The top forums as the instant text (max(numberOfMembers) per forum; no order through DISTINCT),
// as node keys (gid). (2, ARG topForums) Neo4j's first arm: from the top forums' posts down the co-valid reply walk
// (anchored in the same MATCH, so the recursion starts at those posts) to the messages whose creator is a member of
// a top forum, count(DISTINCT message) per person version, as [key, count]. (3, ARG pc) Neo4j's second arm: the
// members with a co-valid membership, each version with its count from (2) or 0 (UNWIND and a conditional sum, no
// lambda), then summed per returned person columns as Neo4j's grouping. (The earlier one-statement form walked
// up from every member's messages and filled the buffer pool at SF0.1.)
MATCH (country:Country)<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(person:Person)<-[hm:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date AND W_OK(country, ip, c, il, person, hm, forum)
WITH country, forum, count(person) AS numberOfMembers
WITH forum, max(numberOfMembers) AS m
RETURN forum.gid AS g, m, forum.id AS fid
ORDER BY m DESC, fid ASC
LIMIT 100
// --- ARG topForums
MATCH (topForum1:Forum)-[cf:CONTAINER_OF]->(post:Post), w = (post)<-[:REPLY_OF* TRAIL 0..30 (_r, _n | WHERE WPRUNE(_r, _n))]-(message:Post:Comment), (message)-[hc:HAS_CREATOR]->(person:Person)<-[hm2:HAS_MEMBER]-(topForum2:Forum)
WHERE topForum1.gid IN $topForums AND topForum2.gid IN $topForums AND W_OK(cf, post, hc, person, hm2 | nodes(w) + relationships(w))
WITH person.gid AS g, count(DISTINCT message) AS n
RETURN [g, n] AS pc
// --- ARG pc
MATCH (person:Person)<-[hm3:HAS_MEMBER]-(f3:Forum)
WHERE f3.gid IN $topForums AND W_OK(person, hm3)
WITH DISTINCT person
UNWIND list_concat($pc, [[CAST(-1 AS INT64), CAST(0 AS INT64)]]) AS x
WITH person, sum(CASE WHEN x[1] = person.gid THEN x[2] ELSE 0 END) AS mc
WITH person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName,
  person.creationDate AS personCreationDate, sum(mc) AS messageCount
RETURN personId, personFirstName, personLastName, personCreationDate, messageCount
ORDER BY messageCount DESC, personId ASC
LIMIT 100
