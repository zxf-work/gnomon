// IC12 for LadybugDB (window): written by scripts/w3_ladybug_texts.py from window/IC12.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R2, R5.
MATCH (p0:Person {id: $personId })-[k:KNOWS]-(friend:Person)<-[h:HAS_CREATOR]-(comment:Comment)-[r:REPLY_OF]->(post:Post)-[ht:HAS_TAG]->(tag:Tag),
      w = (tag)-[:HAS_TYPE|IS_SUBCLASS_OF* TRAIL 0..30 (_r, _n | WHERE WPRUNE(_r, _n))]->(baseTagClass:TagClass)
WHERE (tag.name = $tagClassName OR baseTagClass.name = $tagClassName)
  AND W_OK(p0, k, friend, h, comment, r, post, ht, tag | nodes(w) + rels(w))
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY replyCount DESC, CAST(personId AS INT64) ASC
LIMIT 20
