// HAND IS4 for LadybugDB (window, rule W1), from cypher/window/IS4.cypher with the alignment of D-W3-5 (the SQL
// reference's columns in the SQL order, the SQL coalesce order), as the instant IS4 text of every graph system.
MATCH (m:Post:Comment {id: $messageId })
WHERE W_OK(m)
RETURN
    coalesce(m.imageFile, m.content, '') AS messageContent,
    m.creationDate AS messageCreationDate
