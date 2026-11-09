/* BI8, window setting (rule W1). Deviation: the interest and message scores are stages over their own valid
   bindings (rule 5); the friends' sum joins the scores on the friendship's key (a stage join). */
WITH Person_interested_in_Tag AS (
    SELECT Person.id AS PersonId
      FROM Person
      JOIN Person_hasInterest_Tag
        ON Person_hasInterest_Tag.PersonId = Person.id
      JOIN Tag
        ON Tag.id = Person_hasInterest_Tag.TagId
       AND Tag.name = :tag
     WHERE W_OK(Person, Person_hasInterest_Tag, Tag)
     GROUP BY Person.id
)
   , Person_Message_score AS (
    SELECT Person.id AS PersonId
         , count(*) AS message_score
      FROM Tag
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.TagId = Tag.id
      JOIN Message
        ON Message_hasTag_Tag.MessageId = Message.MessageId
       AND :startDate < Message.creationDate
      JOIN Person
        ON Person.id = Message.CreatorPersonId
     WHERE Tag.name = :tag
       AND Message.creationDate < :endDate
       AND W_OK(Tag, Message_hasTag_Tag, Message, Person)
     GROUP BY Person.id
)
   , Person_score AS (
    SELECT coalesce(Person_interested_in_Tag.PersonId, pms.PersonId) AS PersonId
         , CASE WHEN Person_interested_in_Tag.PersonId IS NULL then 0 ELSE 100 END
         + coalesce(pms.message_score, 0) AS score
      FROM Person_interested_in_Tag
           FULL JOIN Person_Message_score pms
                  ON Person_interested_in_Tag.PersonId = pms.PersonId
)
SELECT p.PersonId AS "person.id"
     , p.score AS score
     , coalesce(sum(f.score), 0) AS friendsScore
  FROM Person_score p
  LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = p.PersonId
  LEFT JOIN Person_score f
    ON f.PersonId = Person_knows_Person.Person2Id
 GROUP BY p.PersonId, p.score
 ORDER BY p.score + coalesce(sum(f.score), 0) DESC, p.PersonId
 LIMIT 100
;
