/* IS6, window setting (rule W1). Deviation: (lo, hi) carried through the REPLY_OF chain; the root
   post's forum and moderator must be co-valid with the chain. The root is the chain's Post (ParentMessageId NULL). */
WITH RECURSIVE chain(parent, child, lo, hi) as (
    SELECT ParentMessageId, id, W_LO(Message), W_HI(Message)
    FROM Message
    WHERE id = :messageId
    UNION ALL
    SELECT Message.ParentMessageId, Message.id, W_LO(Message, ^c), W_HI(Message, ^c)
    FROM Message, chain c
    WHERE Message.id = c.parent AND W_OK(Message, ^c)
)
SELECT
  Forum.id,
  Forum.title,
  Person.id,
  Person.firstName,
  Person.lastName
FROM chain c, Message, Person, Forum
WHERE c.parent IS NULL AND Message.id = c.child
  AND Message.ContainerForumId = Forum.id
  AND ModeratorPersonId = Person.id
  AND W_OK(Message, Forum, Person, ^c)
;
