// IS1, window setting (rule W1): W_OK over the binding (every element believed at T, one shared instant in W).
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
