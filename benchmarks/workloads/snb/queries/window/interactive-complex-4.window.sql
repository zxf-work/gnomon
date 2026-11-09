/* IC4, window setting (rule W1). Deviation: + the start Person p0 and W_OK over each (tag, post, tag edge,
   friendship) binding; the "tag used before" anti-condition is W_NOT (some instant of the binding has no such earlier post). */
SELECT Tag.name, count(*) AS postCount
FROM
    Tag,
    Message,
    Message_hasTag_Tag recent,
    Person_knows_Person,
    Person p0
WHERE Message.id = recent.id
  AND recent.TagId = Tag.id
  AND Message.CreatorPersonId = Person2Id
  AND ParentMessageId IS NULL -- post, not comment
  AND Person1Id = :personId
  AND p0.id = :personId
  AND Message.creationDate >= :startDate
  AND Message.creationDate < (:startDate + INTERVAL '1 days' * :durationDays)
  AND W_OK(Tag, Message, recent, Person_knows_Person, p0)
  AND W_NOT(W_LO(Tag, Message, recent, Person_knows_Person, p0), W_HI(Tag, Message, recent, Person_knows_Person, p0),
        (FROM Message m0, Message_hasTag_Tag t0, Person_knows_Person k0
        WHERE k0.Person1Id = :personId
          AND k0.Person2Id = m0.CreatorPersonId
          AND m0.ParentMessageId IS NULL
          AND t0.id = m0.id
          AND t0.TagId = recent.TagId
          AND m0.creationDate < :startDate), (m0, t0, k0))
GROUP BY Tag.name
ORDER BY postCount DESC, Tag.name ASC
LIMIT 10
;
