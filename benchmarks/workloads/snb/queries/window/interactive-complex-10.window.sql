/* IC10, window setting (rule W1). Deviation: a binding is (person, city, a co-valid 2-hop KNOWS walk from the start
   person, carried with its own interval: W_WALKS) at an instant where the two are not friends (W_NOT). The two
   counts are correlated parts of that binding (G11 verdict 3): a post counts when it shares such an instant with the
   person, the city and a walk (the common one with a tag edge and an interest, the uncommon one when, at that same
   instant, it carries no tag of interest: W_NOTN). */
SELECT
    Person.id,
    Person.firstName,
    Person.lastName,
    (
        SELECT count(DISTINCT m.id)
        FROM Message m, Message_hasTag_Tag pt1, Person_hasInterest_Tag hi, W_WALKS(:personId, 2, 2) w
        WHERE m.CreatorPersonId = Person.id
          AND m.ParentMessageId IS NULL
          AND m.id = pt1.id
          AND hi.PersonId = :personId AND hi.TagId = pt1.TagId
          AND w.f = Person.id
          AND W_OK(Person, City, ^w, m, pt1, hi)
          AND W_NOT(W_LO(Person, City, ^w, m, pt1, hi), W_HI(Person, City, ^w, m, pt1, hi),
              (FROM Person_knows_Person kd, Person q0 WHERE kd.Person1Id = :personId AND kd.Person2Id = Person.id AND q0.id = :personId), (kd, q0))
    ) -
    (
        SELECT count(DISTINCT m.id)
        FROM Message m, W_WALKS(:personId, 2, 2) w
        WHERE m.CreatorPersonId = Person.id
          AND m.ParentMessageId IS NULL
          AND w.f = Person.id
          AND W_OK(Person, City, ^w, m)
          AND W_NOTN(W_LO(Person, City, ^w, m), W_HI(Person, City, ^w, m),
              (FROM Person_knows_Person kd, Person q0 WHERE kd.Person1Id = :personId AND kd.Person2Id = Person.id AND q0.id = :personId), (kd, q0),
              (FROM Person_hasInterest_Tag hi, Message_hasTag_Tag mt WHERE hi.PersonId = :personId AND hi.TagId = mt.TagId AND mt.id = m.id), (hi, mt))
    ) AS commonInterestScore,
    Person.gender,
    City.name
FROM Person, City
WHERE Person.LocationCityId = City.id
  AND Person.id <> :personId
  AND W_OK(Person, City)
  AND EXISTS (SELECT 1 FROM W_WALKS(:personId, 2, 2) w0
              WHERE w0.f = Person.id AND W_OK(Person, City, ^w0)
                AND W_NOT(W_LO(Person, City, ^w0), W_HI(Person, City, ^w0),
                    (FROM Person_knows_Person kd, Person q0 WHERE kd.Person1Id = :personId AND kd.Person2Id = Person.id AND q0.id = :personId), (kd, q0)))
  AND (
      (extract(month FROM Person.birthday) = :month          AND (CASE WHEN extract(day FROM Person.birthday) >= 21 THEN true ELSE false END))
   OR (extract(month FROM Person.birthday) = :month % 12 + 1 AND (CASE WHEN extract(day FROM Person.birthday) <  22 THEN true ELSE false END))
  )
ORDER BY commonInterestScore DESC, Person.id
LIMIT 10
;
