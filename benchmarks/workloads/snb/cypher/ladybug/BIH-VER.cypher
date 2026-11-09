// BIH-VER for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BIH-VER.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
MATCH (p:Person {id: $personId})-[k:KNOWS]-(f:Person)
WHERE k.ets <= $T AND k.evs <= $V AND $V < k.eve
RETURN DISTINCT f.id AS friendId, k.ets AS tt_s,
       CASE WHEN k.ete > $T THEN 4611686018427387904 ELSE k.ete END AS tt_e
ORDER BY friendId, tt_s
