// T-LDBC IS3 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
// AeonG-only workaround (D-W3-10): the undirected KNOWS step is read as its two directions, `->` and `<-`,
// because under TT AS AeonG's undirected expand binds the far node to the start node on incoming
// relationships (PROBLEMS P14, job 49477844); each directed form is correct. (collect drops the nulls of an
// OPTIONAL MATCH with no match; AeonG's Memgraph v2.3 base has no list comprehension over an aggregate.)
MATCH (n:Person {id: $personId })
WHERE VISTT(n) TT AS $AT
OPTIONAL MATCH (n)-[r1:KNOWS]->(f1)
WHERE VISTT(r1, f1) TT AS $AT
WITH n, collect(CASE WHEN r1 IS NULL THEN null ELSE {friend: f1, r: r1} END) AS outs
OPTIONAL MATCH (n)<-[r2:KNOWS]-(f2)
WHERE VISTT(r2, f2) TT AS $AT
WITH outs + collect(CASE WHEN r2 IS NULL THEN null ELSE {friend: f2, r: r2} END) AS pairs
UNWIND pairs AS pr
WITH pr.friend AS friend, pr.r AS r
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    toInteger(personId) ASC
