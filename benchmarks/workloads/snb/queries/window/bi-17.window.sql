/* BI17, window setting (rule W1). Deviation: one binding of message1 (with its tag), message2 (with its tag), the
   comment (with its tag) and both memberships, all co-valid (W_OK); the "person1 not a member of forum2"
   anti-condition is W_NOT. */
SELECT Message1.CreatorPersonId AS "person1.id", count(DISTINCT Message2.MessageId) AS messageCount
FROM Message Message1
JOIN Message_hasTag_Tag t1 ON t1.MessageId = Message1.MessageId
JOIN Tag g1 ON g1.id = t1.TagId AND g1.name = :tag
JOIN Message Message2
 ON (Message1.creationDate + ':delta hour'::interval) < Message2.creationDate
JOIN Message_hasTag_Tag t2 ON t2.MessageId = Message2.MessageId
JOIN Tag g2 ON g2.id = t2.TagId AND g2.name = :tag
JOIN Message Comment
 ON Comment.ParentMessageId = Message2.MessageId
JOIN Message_hasTag_Tag t3 ON t3.MessageId = Comment.MessageId
JOIN Tag g3 ON g3.id = t3.TagId AND g3.name = :tag
JOIN Forum_hasMember_Person Forum_hasMember_Person2
  ON Forum_hasMember_Person2.ForumId = Message1.ContainerForumId
 AND Forum_hasMember_Person2.PersonId = Comment.CreatorPersonId
JOIN Forum_hasMember_Person Forum_hasMember_Person3
  ON Forum_hasMember_Person3.ForumId = Message1.ContainerForumId
 AND Forum_hasMember_Person3.PersonId = Message2.CreatorPersonId
WHERE Message1.ContainerForumId <> Message2.ContainerForumId
  AND Forum_hasMember_Person2.PersonId <> Forum_hasMember_Person3.PersonId
  AND W_OK(Message1, t1, g1, Message2, t2, g2, Comment, t3, g3, Forum_hasMember_Person2, Forum_hasMember_Person3)
  AND W_NOT(W_LO(Message1, t1, g1, Message2, t2, g2, Comment, t3, g3, Forum_hasMember_Person2, Forum_hasMember_Person3),
            W_HI(Message1, t1, g1, Message2, t2, g2, Comment, t3, g3, Forum_hasMember_Person2, Forum_hasMember_Person3),
            (FROM Forum_hasMember_Person Forum_hasMember_Person1
             WHERE Forum_hasMember_Person1.ForumId = Message2.ContainerForumId
               AND Forum_hasMember_Person1.PersonId = Message1.CreatorPersonId), (Forum_hasMember_Person1))
GROUP BY Message1.CreatorPersonId
ORDER BY messageCount DESC, Message1.CreatorPersonId ASC
LIMIT 10
;
