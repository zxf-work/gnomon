// BIH-R3 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BIH-R3.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
UNWIND range(0, ($B - $A - 1) / 86400000000) AS i
WITH $A + i * 86400000000 AS d
OPTIONAL MATCH (a:Person)-[k:KNOWS]->(b:Person)
WHERE k.ets <= $T AND $T < k.ete AND k.evs <= d AND d < k.eve
WITH d, CASE WHEN k IS NULL THEN NULL ELSE [a.id, b.id, k.evs, k.eve, k.ets, k.ete] END AS ev
RETURN d AS bucket, count(DISTINCT ev) AS n
ORDER BY bucket
