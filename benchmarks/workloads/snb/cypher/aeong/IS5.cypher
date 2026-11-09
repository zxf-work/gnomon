// T-LDBC IS5 for AeonG, native transaction-time form: AeonG's own clock carries belief time (TT AS),
// valid time is a property predicate. Deviation from LDBC: + VISTT on every variable, + TT AS per MATCH.
MATCH (m:Message {id:  $messageId })-[h:HAS_CREATOR]->(p:Person)
WHERE VISTT(m, h, p) TT AS $AT
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
