/* BI7, window setting (rule W1). Deviation: the (parent tag edge, tag, comment, comment tag edge, related tag)
   binding is co-valid; "the comment does not carry the tag" is W_NOT. */
SELECT RelatedTag.name AS "relatedTag.name"
     , count(*) AS count
  FROM Message_hasTag_Tag pt
  JOIN Tag ON Tag.id = pt.TagId AND Tag.name = :tag
  INNER JOIN Message Comment
          ON pt.MessageId = Comment.ParentMessageId
  INNER JOIN Message_hasTag_Tag ct
          ON Comment.MessageId = ct.MessageId
  INNER JOIN Tag RelatedTag
          ON RelatedTag.id = ct.TagId
 WHERE Comment.ParentMessageId IS NOT NULL
   AND W_OK(pt, Tag, Comment, ct, RelatedTag)
   AND W_NOT(W_LO(pt, Tag, Comment, ct, RelatedTag), W_HI(pt, Tag, Comment, ct, RelatedTag),
             (FROM Message_hasTag_Tag x, Tag xt WHERE x.MessageId = Comment.MessageId AND x.TagId = xt.id AND xt.name = :tag), (x, xt))
 GROUP BY RelatedTag.Name
 ORDER BY count DESC, RelatedTag.name
 LIMIT 100
;
