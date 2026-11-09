/* BI16, window setting (rule W1). Deviation: subgraphA/B are stages over valid (person, message, tag edge, tag)
   bindings; the induced friendships are joined on the stage's person key and must be valid in the window view
   (rule 5). */
WITH
  subgraphA AS (
    SELECT DISTINCT Person.id AS PersonId, Message.MessageId AS MessageId
    FROM Person
    JOIN Message
      ON Message.CreatorPersonId = Person.id
     AND Message.creationDate::date = :dateA
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.MessageId = Message.MessageId
    JOIN Tag
      ON Tag.id = Message_hasTag_Tag.TagId
     AND Tag.name = :tagA
    WHERE W_OK(Person, Message, Message_hasTag_Tag, Tag)
  ),
  personA AS (
    SELECT
        subgraphA1.PersonId,
        count(DISTINCT subgraphA1.MessageId) AS cm,
        count(DISTINCT Person_knows_Person.Person2Id) AS cp2
    FROM subgraphA subgraphA1
    LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = subgraphA1.PersonId
    AND Person_knows_Person.Person2Id IN (SELECT PersonId FROM subgraphA)
    GROUP BY subgraphA1.PersonId
    HAVING count(DISTINCT Person_knows_Person.Person2Id) <= :maxKnowsLimit
  ),
  subgraphB AS (
    SELECT DISTINCT Person.id AS PersonId, Message.MessageId AS MessageId
    FROM Person
    JOIN Message
      ON Message.CreatorPersonId = Person.id
     AND Message.creationDate::date = :dateB
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.MessageId = Message.MessageId
    JOIN Tag
      ON Tag.id = Message_hasTag_Tag.TagId
     AND Tag.name = :tagB
    WHERE W_OK(Person, Message, Message_hasTag_Tag, Tag)
  ),
  personB AS (
    SELECT
        subgraphB1.PersonId,
        count(DISTINCT subgraphB1.MessageId) AS cm,
        count(DISTINCT Person_knows_Person.Person2Id) AS cp2
    FROM subgraphB subgraphB1
    LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = subgraphB1.PersonId
    AND Person_knows_Person.Person2Id IN (SELECT PersonId FROM subgraphB)
    GROUP BY subgraphB1.PersonId
    HAVING count(DISTINCT Person_knows_Person.Person2Id) <= :maxKnowsLimit
  )
SELECT
    personA.PersonId AS PersonId,
    personA.cm AS messageCountA,
    personB.cm AS messageCountB
FROM personA
JOIN personB
  ON personB.PersonId = personA.PersonId
ORDER BY personA.cm + personB.cm DESC, PersonId ASC
LIMIT 20
;
