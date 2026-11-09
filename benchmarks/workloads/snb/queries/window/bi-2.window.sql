/* BI2, window setting (rule W1). Deviation: the (tag class, tag, tag edge, message) binding is co-valid (W_OK);
   the tag list is a stage of its own (rule 5). */
WITH
MyTag AS (
SELECT Tag.id AS id, Tag.name AS name
  FROM TagClass
  JOIN Tag
    ON Tag.TypeTagClassId = TagClass.id
 WHERE TagClass.name = :tagClass AND W_OK(TagClass, Tag)
),
detail AS (
SELECT Tag.id as TagId
     , count(CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth1
     , count(CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth2
  FROM TagClass
  JOIN Tag ON Tag.TypeTagClassId = TagClass.id
  JOIN Message_hasTag_Tag
         ON Message_hasTag_tag.TagId = Tag.id
  JOIN Message
    ON Message.MessageId = Message_hasTag_tag.MessageId
   AND Message.creationDate >= :date
   AND Message.creationDate <  :date + INTERVAL '200 days'
 WHERE TagClass.name = :tagClass AND W_OK(TagClass, Tag, Message_hasTag_Tag, Message)
 GROUP BY Tag.id
)
SELECT t.name AS "tag.name"
     , coalesce(countMonth1, 0)
     , coalesce(countMonth2, 0)
     , abs(coalesce(countMonth1, 0)-coalesce(countMonth2, 0)) AS diff
  FROM MyTag t LEFT JOIN detail ON t.id = detail.TagId
 ORDER BY diff desc, t.name
 LIMIT 100
;
