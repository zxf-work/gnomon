// IS4, window setting (rule W1).
MATCH (m:Message {id: $messageId })
WHERE W_OK(m)
RETURN
    m.creationDate as messageCreationDate,
    coalesce(m.content, m.imageFile) as messageContent
