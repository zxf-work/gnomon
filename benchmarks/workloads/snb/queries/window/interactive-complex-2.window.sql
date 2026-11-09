/* IC2, window setting (rule W1). Deviation: + the start Person p0 and W_OK over the binding. */
SELECT
    Person.id,
    Person.firstName,
    Person.lastName,
    Message.id,
    coalesce(Message.imageFile, Message.content),
    Message.creationDate
FROM
    Person,
    Message,
    Person_knows_Person,
    Person p0
WHERE Person.id = Message.CreatorPersonId
  AND Message.creationDate < :maxDate
  AND Person1Id = :personId
  AND Person2Id = Person.id
  AND p0.id = :personId
  AND W_OK(Person, Message, Person_knows_Person, p0)
ORDER BY creationDate DESC, Message.id ASC
LIMIT 20
;
