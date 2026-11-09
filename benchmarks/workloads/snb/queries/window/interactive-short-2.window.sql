/* IS2, window setting (rule W1). Deviation: the binding interval (lo, hi) is carried through the
   REPLY_OF recursion; the last 10 messages are chosen among co-valid (person, message) bindings. */
WITH RECURSIVE cposts(id, content, imageFile, creationDate, ParentMessageId, CreatorPersonId, lo, hi) AS (
    SELECT m.id, m.content, m.imageFile, m.creationDate, m.ParentMessageId, m.CreatorPersonId, W_LO(m, p0), W_HI(m, p0)
    FROM Message m, Person p0
    WHERE m.CreatorPersonId = :personId AND p0.id = :personId AND W_OK(m, p0)
    ORDER BY m.creationDate DESC
    LIMIT 10
), parent(postid, ParentMessageId, orig_postid, CreatorPersonId, lo, hi) AS (
    SELECT id, ParentMessageId, id, CreatorPersonId, lo, hi
    FROM cposts
    UNION ALL
    SELECT Message.id, Message.ParentMessageId, parent.orig_postid, Message.CreatorPersonId, W_LO(Message, ^parent), W_HI(Message, ^parent)
    FROM Message, parent
    WHERE Message.id = parent.ParentMessageId AND W_OK(Message, ^parent)
)
SELECT
    m1.id,
    coalesce(imageFile, content, ''),
    m1.creationDate,
    m2.id,
    m2.PersonId,
    m2.firstName,
    m2.lastName
FROM
    (SELECT id, content, imageFile, creationDate, ParentMessageId FROM cposts) m1
LEFT JOIN
    (
        SELECT orig_postid, postid AS id, Person.id AS PersonId, firstName, lastName
        FROM parent, Person
        WHERE ParentMessageId IS NULL AND parent.CreatorPersonId = Person.id AND W_OK(Person, ^parent)
    ) m2
    ON m2.orig_postid = m1.id
ORDER BY creationDate DESC, m2.id DESC
;
