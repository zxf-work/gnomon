/* BI5, window setting (rule W1). Deviation: the reply and like counts are stages over their own valid bindings
   (rule 5); the (tag, tag edge, message) binding is co-valid (W_OK). */
WITH detail AS (
SELECT Message.CreatorPersonId AS CreatorPersonId
     , sum(coalesce(Cs.c, 0))  AS replyCount
     , sum(coalesce(Plm.c, 0)) AS likeCount
     , count(Message.MessageId) AS messageCount
  FROM Tag
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.TagId = Tag.id
  JOIN Message
    ON Message.MessageId = Message_hasTag_Tag.MessageId
  LEFT JOIN (SELECT c.ParentMessageId, count(*) FROM Message c WHERE c.ParentMessageId IS NOT NULL GROUP BY c.ParentMessageId) Cs(id, c) ON Cs.id = Message.MessageId
  LEFT JOIN (SELECT MessageId, count(*) FROM Person_likes_Message GROUP BY MessageId) Plm(id, c) ON Plm.id = Message.MessageId
 WHERE Tag.name = :tag AND W_OK(Tag, Message_hasTag_Tag, Message)
 GROUP BY Message.CreatorPersonId
)
SELECT CreatorPersonId AS "person.id"
     , replyCount
     , likeCount
     , messageCount
     , 1*messageCount + 2*replyCount + 10*likeCount AS score
  FROM detail
 ORDER BY score DESC, CreatorPersonId
 LIMIT 100
;
