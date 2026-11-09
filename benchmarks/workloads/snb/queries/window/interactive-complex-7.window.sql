/* IC7, window setting (rule W1). Deviation: each like binding (liker, like, message, start person p0) is
   co-valid; the latest like per liker is taken over valid bindings; isNew follows the optional reading O1:
   0 for a co-valid friendship, 1 when some instant of the binding has none (W_NOT); a like can appear with both. */
SELECT
    Person.id,
    Person.firstName,
    Person.lastName,
    l.creationDate,
    Message.id,
    coalesce(imageFile, content),
    CAST(floor(EXTRACT(EPOCH FROM (l.creationDate - Message.creationDate))) AS INTEGER) / 60 AS minutesLatency,
    nw.isNew
FROM
    (
        SELECT l2.PersonId, max(l2.creationDate) AS creationDate
        FROM Person_likes_Message l2, Message m2, Person lp, Person q0
        WHERE l2.id = m2.id AND m2.CreatorPersonId = :personId AND lp.id = l2.PersonId AND q0.id = :personId
          AND W_OK(l2, m2, lp, q0)
        GROUP BY l2.PersonId
        ORDER BY creationDate DESC, l2.PersonId ASC
        LIMIT 20
    ) tmp,
    Message,
    Person,
    Person p0,
    Person_likes_Message AS l,
    LATERAL (
        SELECT 0 AS isNew WHERE W_HOP(:personId, Person.id, 1, W_LO(l, Message, Person, p0), W_HI(l, Message, Person, p0))
        UNION ALL
        SELECT 1 WHERE W_NOT(W_LO(l, Message, Person, p0), W_HI(l, Message, Person, p0),
            (FROM Person_knows_Person kd, Person q0 WHERE kd.Person1Id = :personId AND kd.Person2Id = Person.id AND q0.id = :personId), (kd, q0))
    ) nw
WHERE Person.id = tmp.PersonId
  AND tmp.PersonId = l.PersonId
  AND tmp.creationDate = l.creationDate
  AND l.id = Message.id
  AND Message.CreatorPersonId = :personId
  AND p0.id = :personId
  AND W_OK(l, Message, Person, p0)
ORDER BY l.creationDate DESC, Person.id ASC, nw.isNew
;
