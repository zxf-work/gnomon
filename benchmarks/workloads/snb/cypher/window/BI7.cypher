// BI7, window setting (rule W1): one co-valid binding (tag, tag edge, message, reply edge, comment, tag edge,
// related tag); "the comment does not carry the tag" is W1's NOT (W_UNCOVERED).
MATCH
  (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Message),
  (message)<-[ro:REPLY_OF]-(comment:Comment)-[hr:HAS_TAG]->(relatedTag:Tag)
WHERE W_OK(tag, ht, message, ro, comment, hr, relatedTag)
WITH tag, comment, relatedTag, W_LOV(tag, ht, message, ro, comment, hr, relatedTag) AS lo, W_HIV(tag, ht, message, ro, comment, hr, relatedTag) AS hi
OPTIONAL MATCH (comment)-[hx:HAS_TAG]->(tx:Tag {name: $tag})
WHERE W_INV(lo, hi, hx, tx)
WITH relatedTag, comment, lo, hi, collect(CASE WHEN hx IS NULL THEN null ELSE W_EXT(hx, tx) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN relatedTag.name, count(DISTINCT comment) AS count
ORDER BY count DESC, relatedTag.name ASC
LIMIT 100
