// HAND BI4 for LadybugDB (instant settings), from cypher/BI4.cypher. Changes (D-W3-13), in rounds (each statement's
// first column is the next one's list parameter):
// (1) the top 100 forums: Neo4j orders (forum, country) rows and keeps each forum's first row (WITH DISTINCT after
//     ORDER BY), which is the forum's largest member count: forums by max(numberOfMembers) DESC, forum.id ASC,
//     returned as rows (LB-9: an aggregation right after WITH ... ORDER BY ... LIMIT ignores the order);
// (2) the top forums' posts (container edge and post visible); (3) the messages of their threads (the post itself
//     and every visible reply walk to it; the walk's source filtered in its own MATCH, as LadybugDB needs);
// (4) no CALL {}: the UNION ALL of (member, 0) rows and (member, message count) rows is the members of the top
//     forums, each with the count of distinct thread messages it created through a visible creator edge. The second
//     arm's membership test is the first arm's (a person with a visible membership in a top forum), so the rows and
//     the sums are Neo4j's. Persons are carried as values (LB-6).
MATCH (country:Country)<-[ip:IS_PART_OF]-(c:City)<-[il:IS_LOCATED_IN]-(person:Person)<-[hm:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date AND VIS(country, ip, c, il, person, hm, forum)
WITH country, forum, count(person) AS numberOfMembers
WITH forum, max(numberOfMembers) AS m
RETURN forum.gid AS g, m, forum.id AS fid
ORDER BY m DESC, fid ASC
LIMIT 100
// --- ARG topForums
MATCH (f:Forum)-[cf:CONTAINER_OF]->(post:Post)
WHERE f.gid IN $topForums AND VIS(cf, post)
RETURN DISTINCT post.gid
// --- ARG posts
MATCH (post:Post)<-[:REPLY_OF* TRAIL 1..30 (_r, _n | WHERE VIS(_r, _n))]-(message:Comment)
WHERE post.gid IN $posts AND VIS(message)
RETURN DISTINCT message.gid
// --- ARG replies
MATCH (person:Person)<-[hm3:HAS_MEMBER]-(f3:Forum)
WHERE f3.gid IN $topForums AND VIS(person, hm3)
WITH DISTINCT person.gid AS pg, person.id AS pid, person.firstName AS pfn, person.lastName AS pln, person.creationDate AS pcd
OPTIONAL MATCH (m:Post:Comment)-[hc:HAS_CREATOR]->(p:Person)
WHERE p.gid = pg AND (m.gid IN $posts OR m.gid IN $replies) AND VIS(hc)
WITH pid, pfn, pln, pcd, count(DISTINCT m.gid) AS messageCount
RETURN pid AS personId, pfn AS personFirstName, pln AS personLastName, pcd AS personCreationDate, messageCount
ORDER BY messageCount DESC, personId ASC
LIMIT 100
