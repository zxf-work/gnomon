// IS5 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IS5.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1.
MATCH (m:Post:Comment {id:  $messageId })-[h:HAS_CREATOR]->(p:Person)
WHERE W_OK(m, h, p)
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
