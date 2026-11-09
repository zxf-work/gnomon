/* IS7, window setting (rule W1, optional reading O1). Deviation: W_OK over (p1, p2, Person); the "reply author
   knows the original author" flag is the reference's optional part: a row with true for a co-valid
   friendship (with the author's version), and a row with false when some instant of the binding has none
   (W_NOT), so a reply can appear with both. */
SELECT p2.id, p2.content, p2.creationDate, Person.id, Person.firstName, Person.lastName, flag
FROM Message p1, Message p2, Person,
LATERAL (
    SELECT true AS flag WHERE EXISTS (
        SELECT 1 FROM Person_knows_Person k, Person a
        WHERE p1.CreatorPersonId = k.Person1Id AND p2.CreatorPersonId = k.Person2Id AND a.id = p1.CreatorPersonId
          AND W_IN(W_LO(p1, p2, Person), W_HI(p1, p2, Person), k, a))
    UNION ALL
    SELECT false WHERE W_NOT(W_LO(p1, p2, Person), W_HI(p1, p2, Person),
        (FROM Person_knows_Person k, Person a
         WHERE p1.CreatorPersonId = k.Person1Id AND p2.CreatorPersonId = k.Person2Id AND a.id = p1.CreatorPersonId), (k, a))
) f
WHERE p1.id = :messageId
  AND p2.ParentMessageId = p1.id
  AND p2.CreatorPersonId = Person.id
  AND W_OK(p1, p2, Person)
ORDER BY p2.creationDate DESC, p2.CreatorPersonId ASC, flag
;
