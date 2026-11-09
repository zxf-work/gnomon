/* IC8, window setting (rule W1). Deviation: + the start Person p0 and W_OK over the binding. */
SELECT
    m1.CreatorPersonId,
    Person.firstName,
    Person.lastName,
    m1.creationDate,
    m1.id,
    m1.content
FROM
    Message m1,
    Message m2,
    Person,
    Person p0
WHERE m1.ParentMessageId = m2.id
  AND m2.CreatorPersonId = :personId
  AND Person.Id = m1.CreatorPersonId
  AND p0.id = :personId
  AND W_OK(m1, m2, Person, p0)
ORDER BY
    m1.creationDate DESC,
    m1.id ASC
LIMIT 20
;
