// T-LDBC IS1 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
// Deviation: + VISTT(...) on every bound variable (W3 four-timestamp predicate).
MATCH (n:Person {id: $personId })-[r:IS_LOCATED_IN]->(p:City)
WHERE VISTT(n, r, p) TT AS $AT
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate
