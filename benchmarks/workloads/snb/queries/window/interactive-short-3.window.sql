/* IS3, window setting (rule W1). Deviation: + the start Person p0 version and W_OK over the binding. */
SELECT Person.id, Person.firstName, person.lastName, k.creationDate
FROM Person_knows_Person k, Person, Person p0
WHERE k.Person1Id = :personId
  AND k.Person2Id = Person.id
  AND p0.id = :personId
  AND W_OK(k, Person, p0)
ORDER BY k.creationDate DESC, Person.id ASC
;
