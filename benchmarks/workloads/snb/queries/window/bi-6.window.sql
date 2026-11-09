/* BI6, window setting (rule W1). Deviation: Umbra's precomputed PopularityScoreQ06 is a stage over its own valid
   bindings (rule 5); the (tag, tag edge, message, like) binding is co-valid, the like being an optional part (W_IN). */
WITH PopularityScore AS (
    SELECT message2.CreatorPersonId AS person2id, count(*) AS popularityScore
    FROM Message message2 JOIN Person_likes_Message like2 ON like2.MessageId = message2.MessageId
    WHERE W_OK(message2, like2)
    GROUP BY message2.CreatorPersonId
), poster_w_liker AS (
        SELECT DISTINCT
            message1.CreatorPersonId AS person1id,
            like2.PersonId AS person2id
        FROM Tag
        JOIN Message_hasTag_Tag
          ON Message_hasTag_Tag.TagId = Tag.id
        JOIN Message message1
          ON message1.MessageId = Message_hasTag_Tag.MessageId
        LEFT JOIN Person_likes_Message like2
               ON like2.MessageId = message1.MessageId
              AND W_IN(W_LO(Tag, Message_hasTag_Tag, message1), W_HI(Tag, Message_hasTag_Tag, message1), like2)
         WHERE Tag.name = :tag AND W_OK(Tag, Message_hasTag_Tag, message1)
    )
SELECT pl.person1id AS "person1.id",
       sum(coalesce(ps.popularityScore, 0)) AS authorityScore
FROM poster_w_liker pl
LEFT JOIN PopularityScore ps
         ON ps.person2id = pl.person2id
GROUP BY pl.person1id
ORDER BY authorityScore DESC, pl.person1id ASC
LIMIT 100
;
