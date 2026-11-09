/* BI10, window setting (rule W1). Deviation: the friend band is W_BAND: at some instant of the (friend, city,
   country, message, tag edge, tag, tag class) binding, the least depth of a co-valid KNOWS walk is in
   [minPathDistance, maxPathDistance] (per instant, G11 verdict 2); the per-tag count joins the message's tags as a
   stage on the message key (rule 5). */
WITH messages_of_tagclass_by_friends AS (
    SELECT DISTINCT tf.id AS friendId, Message.MessageId AS messageid
      FROM Person tf
      JOIN City ON City.id = tf.LocationCityId
      JOIN Country ON Country.id = City.PartOfCountryId AND Country.name = :country
      JOIN Message ON Message.CreatorPersonId = tf.id
      JOIN Message_hasTag_Tag ON Message_hasTag_Tag.MessageId = Message.MessageId
      JOIN Tag ON Tag.id = Message_hasTag_Tag.TagId
      JOIN TagClass ON TagClass.id = Tag.TypeTagClassId
     WHERE TagClass.name = :tagClass
       AND tf.id <> :personId
       AND W_OK(tf, City, Country, Message, Message_hasTag_Tag, Tag, TagClass)
       AND W_BAND(:personId, tf.id, 4, :minPathDistance, :maxPathDistance, W_LO(tf, City, Country, Message, Message_hasTag_Tag, Tag, TagClass),
                  W_HI(tf, City, Country, Message, Message_hasTag_Tag, Tag, TagClass))
)
SELECT m.friendId AS "person.id"
     , Tag.name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.MessageId = m.MessageId
  JOIN Tag
    ON Tag.id = Message_hasTag_Tag.TagId
 WHERE W_OK(Message_hasTag_Tag, Tag)
 GROUP BY m.friendId, Tag.name
 ORDER BY messageCount DESC, Tag.name, m.friendId
 LIMIT 100
;
