/* IS5, window setting (rule W1). Deviation: + W_OK over the binding. */
SELECT Person.id, Person.firstName, Person.lastName
FROM Message, Person
WHERE Message.id = :messageId
  AND Message.CreatorPersonId = Person.id
  AND W_OK(Message, Person)
;
