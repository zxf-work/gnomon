/* IC5, window setting (rule W1). Deviation: the friend set is W_REACH (1..2 hops) against the (forum, membership)
   binding; a left-joined post counts when it shares an instant with the forum, the membership and a co-valid walk
   (the walk's interval is carried: W_REACH over the joint interval inside the count, G11 verdict 1). */
SELECT
    Forum.title,
    count(DISTINCT CASE WHEN W_REACH(:personId, Forum_hasMember_Person.PersonId, 2, W_LO(Forum, Forum_hasMember_Person, m),
                                     W_HI(Forum, Forum_hasMember_Person, m)) THEN m.id END) AS postCount
FROM Forum
JOIN Forum_hasMember_Person ON Forum.id = Forum_hasMember_Person.ForumId
LEFT JOIN Message m
       ON Forum.id = m.ContainerForumId
      AND m.CreatorPersonId = Forum_hasMember_Person.PersonId
      AND m.ParentMessageId IS NULL
      AND W_IN(W_LO(Forum, Forum_hasMember_Person), W_HI(Forum, Forum_hasMember_Person), m)
WHERE Forum_hasMember_Person.creationDate >= :minDate
  AND Forum_hasMember_Person.PersonId <> :personId
  AND W_OK(Forum, Forum_hasMember_Person)
  AND W_REACH(:personId, Forum_hasMember_Person.PersonId, 2, W_LO(Forum, Forum_hasMember_Person), W_HI(Forum, Forum_hasMember_Person))
GROUP BY Forum.id, Forum.title
ORDER BY postCount DESC, Forum.id ASC
LIMIT 20
;
