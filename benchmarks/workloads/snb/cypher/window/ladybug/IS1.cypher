// IS1 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IS1.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
MATCH (n:Person {id: $personId })-[r:IS_LOCATED_IN]->(p:City)
WHERE W_OK(n, r, p)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate
