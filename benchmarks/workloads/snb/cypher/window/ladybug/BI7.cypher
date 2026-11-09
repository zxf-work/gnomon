// BI7 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/BI7.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1, R6 (ht.rid <> hr.rid).
MATCH
  (tag:Tag {name: $tag})<-[ht:HAS_TAG]-(message:Post:Comment),
  (message)<-[ro:REPLY_OF]-(comment:Comment)-[hr:HAS_TAG]->(relatedTag:Tag)
WHERE ht.rid <> hr.rid AND W_OK(tag, ht, message, ro, comment, hr, relatedTag)
WITH tag, comment, relatedTag, W_LOV(tag, ht, message, ro, comment, hr, relatedTag) AS lo, W_HIV(tag, ht, message, ro, comment, hr, relatedTag) AS hi
OPTIONAL MATCH (comment)-[hx:HAS_TAG]->(tx:Tag {name: $tag})
WHERE W_INV(lo, hi, hx, tx)
WITH relatedTag, comment, lo, hi, collect(CASE WHEN hx IS NULL THEN null ELSE W_EXT(hx, tx) END) AS ext
WHERE W_UNCOVERED(lo, hi, ext)
RETURN relatedTag.name, count(DISTINCT comment) AS count
ORDER BY count DESC, relatedTag.name ASC
LIMIT 100
