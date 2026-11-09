// timed-w3 stage override: the two RETURN items in the order of the instant IS4 text and the SQL reference.
// IS4, window setting (rule W1).
MATCH (m:Message {id: $messageId })
WHERE W_OK(m)
RETURN
    coalesce(m.content, m.imageFile) as messageContent,
    m.creationDate as messageCreationDate
