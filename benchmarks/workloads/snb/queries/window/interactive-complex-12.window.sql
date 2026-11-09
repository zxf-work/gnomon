/* IC12, window setting (rule W1). Deviation: W_OK over each (friendship, friend, reply, post, tag edge, tag,
   tag class) binding with the start person p0; the subclass closure carries (lo, hi). */
WITH RECURSIVE extended_tags(s_subtagclassid, s_supertagclassid, lo, hi) AS (
    SELECT id, id, W_LO(tagclass), W_HI(tagclass)
    FROM tagclass
    UNION
    SELECT tc.id, t.s_supertagclassid, W_LO(tc, ^t), W_HI(tc, ^t)
      FROM tagclass tc, extended_tags t
     WHERE tc.SubclassOfTagClassId = t.s_subtagclassid AND W_OK(tc, ^t)
)
SELECT
    Person.id,
    Person.firstName,
    Person.lastName,
    array_agg(DISTINCT tag.name),
    count(DISTINCT m1.id) AS replyCount
FROM
    Person,
    Message m1,
    Person_knows_Person,
    Message m2,
    Message_hasTag_Tag,
    tag,
    extended_tags k,
    tagclass sup,
    Person p0
WHERE Person1Id = :personId
  AND Person2Id = Person.id
  AND Person.id = m1.CreatorPersonId
  AND m1.ParentMessageId = m2.id
  AND m2.ParentMessageId IS NULL
  AND m2.id = Message_hasTag_Tag.id
  AND Message_hasTag_Tag.TagId = tag.id
  AND tag.TypeTagClassId = k.s_subtagclassid
  AND sup.id = k.s_supertagclassid
  AND sup.name = :tagClassName
  AND p0.id = :personId
  AND W_OK(Person, m1, Person_knows_Person, m2, Message_hasTag_Tag, tag, sup, p0, ^k)
GROUP BY Person.id, Person.firstName, Person.lastName
ORDER BY replyCount DESC, Person.id ASC
LIMIT 20
;
