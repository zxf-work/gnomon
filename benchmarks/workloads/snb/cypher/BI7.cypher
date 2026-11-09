// BI7. Deviation: + VIS on every variable; the "comment does not carry the tag" pattern predicate as an
// OPTIONAL MATCH count with VIS.
MATCH
  (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Message),
  (message)<-[ro:REPLY_OF]-(comment:Comment)-[hr:HAS_TAG]->(relatedTag:Tag)
WHERE VIS(tag, ht, message, ro, comment, hr, relatedTag)
OPTIONAL MATCH (comment)-[hx:HAS_TAG]->(tag)
WHERE VIS(hx)
WITH relatedTag, comment, count(hx) AS own
WHERE own = 0
RETURN
  relatedTag.name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  relatedTag.name ASC
LIMIT 100
