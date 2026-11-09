// IS5, window setting (rule W1).
MATCH (m:Message {id:  $messageId })-[h:HAS_CREATOR]->(p:Person)
WHERE W_OK(m, h, p)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
