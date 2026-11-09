/* BI3, window setting (rule W1). Deviation: W_OK over (message, forum, moderator, city, country); the tag-class
   EXISTS is part of the binding (W_IN). */
SELECT Forum.id                AS "forum.id"
     , Forum.title             AS "forum.title"
     , Forum.creationDate      AS "forum.creationDate"
     , Forum.ModeratorPersonId AS "person.id"
     , count(Message.MessageId) AS messageCount
FROM Message
JOIN Forum
  ON Forum.id = Message.ContainerForumId
JOIN Person AS ModeratorPerson
  ON ModeratorPerson.id = Forum.ModeratorPersonId
JOIN City
  ON City.id = ModeratorPerson.LocationCityId
JOIN Country
  ON Country.id = City.PartOfCountryId
 AND Country.name = :country
WHERE W_OK(Message, Forum, ModeratorPerson, City, Country)
  AND EXISTS (
  SELECT 1
    FROM TagClass
    JOIN Tag
      ON Tag.TypeTagClassId = TagClass.id
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.TagId = Tag.id
   WHERE Message.MessageId = Message_hasTag_Tag.MessageId AND TagClass.name = :tagClass
     AND W_IN(W_LO(Message, Forum, ModeratorPerson, City, Country), W_HI(Message, Forum, ModeratorPerson, City, Country), TagClass, Tag, Message_hasTag_Tag))
GROUP BY Forum.id, Forum.title, Forum.creationDate, Forum.ModeratorPersonId
ORDER BY messageCount DESC, Forum.id
LIMIT 20
;
