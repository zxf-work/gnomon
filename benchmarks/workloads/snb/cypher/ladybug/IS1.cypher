// IS1 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IS1.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
MATCH (n:Person {id: $personId })-[r:IS_LOCATED_IN]->(p:City)
WHERE VIS(n, r, p)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate
