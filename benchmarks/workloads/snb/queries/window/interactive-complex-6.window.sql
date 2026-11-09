/* IC6, window setting (rule W1). Deviation: one binding of the post, both tag edges, both tags and a co-valid
   walk (1..2 hops) to its creator: the walk is tested over the joint interval (G11 verdict 1). */
SELECT Tag.name, count(DISTINCT Message.id) AS postcount
FROM Tag, Message_hasTag_Tag, Message
WHERE Message.CreatorPersonId <> :personId
  AND ParentMessageId IS NULL -- post, not comment
  AND Message.id = Message_hasTag_Tag.id
  AND Message_hasTag_Tag.TagId = Tag.id
  AND Tag.name <> :tagName
  AND W_OK(Tag, Message_hasTag_Tag, Message)
  AND EXISTS (
          SELECT 1
          FROM Tag t2, Message_hasTag_Tag mt2
          WHERE Message.id = mt2.id
            AND mt2.TagId = t2.id
            AND t2.name = :tagName
            AND W_REACH(:personId, Message.CreatorPersonId, 2, W_LO(Tag, Message_hasTag_Tag, Message, t2, mt2),
                        W_HI(Tag, Message_hasTag_Tag, Message, t2, mt2))
      )
GROUP BY Tag.name
ORDER BY postCount DESC, Tag.name ASC
LIMIT 10
;
