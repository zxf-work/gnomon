// IS5. Deviation: + VIS on every bound variable.
MATCH (m:Message {id:  $messageId })-[h:HAS_CREATOR]->(p:Person)
WHERE VIS(m, h, p)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
