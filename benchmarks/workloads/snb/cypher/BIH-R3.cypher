// BIH-R3 (Cypher form of queries/bih-R3.sql). The number of KNOWS edge versions valid at the start of each day of
// [$A, $B), as believed at $T. Edge versions by their own rectangle; count(DISTINCT key) folds the copies.
// Ours, not LDBC's.
UNWIND range(0, ($B - $A - 1) / 86400000000) AS i
WITH $A + i * 86400000000 AS d
OPTIONAL MATCH (a:Person)-[k:KNOWS]->(b:Person)
WHERE k.ets <= $T AND $T < k.ete AND k.evs <= d AND d < k.eve
WITH d, CASE WHEN k IS NULL THEN NULL ELSE [a.id, b.id, k.evs, k.eve, k.ets, k.ete] END AS ev
RETURN d AS bucket, count(DISTINCT ev) AS n
ORDER BY bucket
