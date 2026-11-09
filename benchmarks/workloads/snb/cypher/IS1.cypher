// IS1 (ldbc_snb_interactive_v2_impls/cypher interactive-short-1), at one belief setting.
// Deviation: + VIS(...) on every bound variable (W3 four-timestamp predicate).
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
