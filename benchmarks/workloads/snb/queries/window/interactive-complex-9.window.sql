/* IC9, window setting (rule W1). Deviation: the friend / friend-of-friend set becomes W_REACH
   (a co-valid KNOWS walk of 1..2 hops from the start person), tested against the (friend, message) binding. */
SELECT
    Person.id,
    firstName,
    lastName,
    Message.id,
    coalesce(imageFile, content),
    Message.creationDate
FROM
    Person,
    Message
WHERE Person.id = CreatorPersonId
  AND Person.id <> :personId
  AND Message.creationDate < :maxDate
  AND W_OK(Person, Message)
  AND W_REACH(:personId, Person.id, 2, W_LO(Person, Message), W_HI(Person, Message))
ORDER BY Message.creationDate DESC, Message.id ASC
LIMIT 20
;
