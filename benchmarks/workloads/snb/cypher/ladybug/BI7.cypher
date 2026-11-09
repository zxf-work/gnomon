// BI7 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BI7.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R6 (ht.rid <> hr.rid).
MATCH
  (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Post:Comment),
  (message)<-[ro:REPLY_OF]-(comment:Comment)-[hr:HAS_TAG]->(relatedTag:Tag)
WHERE ht.rid <> hr.rid AND VIS(tag, ht, message, ro, comment, hr, relatedTag)
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
