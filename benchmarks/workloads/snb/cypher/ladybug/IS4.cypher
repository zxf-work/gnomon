// IS4 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from IS4.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1.
MATCH (m:Post:Comment {id: $messageId })
WHERE VIS(m)
RETURN
    coalesce(m.imageFile, m.content, '') as messageContent,
    m.creationDate as messageCreationDate
