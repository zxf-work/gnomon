// IS4. Deviation: + VIS; the SQL reference's column order and coalesce order (D-W3-5).
MATCH (m:Message {id: $messageId })
WHERE VIS(m)
RETURN
    coalesce(m.imageFile, m.content, '') as messageContent,
    m.creationDate as messageCreationDate
