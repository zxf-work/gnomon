// HAND BIH-R7 for LadybugDB, from cypher/BIH-R7.cypher. Changes (D-W3-13): the (change, tt) pairs are structs (a
// LadybugDB list holds one type), unwound and filtered.
MATCH (a:Person)-[k:KNOWS]->(b:Person)
WHERE k.evs < $B AND $A < k.eve AND ((k.ets > $T1 AND k.ets <= $T) OR (k.ete > $T1 AND k.ete <= $T))
WITH DISTINCT a.id AS p1, b.id AS p2, k.evs AS vs, k.eve AS ve, k.ets AS ts, k.ete AS te
UNWIND [{change: 'added', tt: ts}, {change: 'retracted', tt: te}] AS ch
WITH p1, p2, vs, ve, ch
WHERE ch.tt > $T1 AND ch.tt <= $T
RETURN p1 AS Person1Id, p2 AS Person2Id, ch.change AS change, ch.tt AS tt, vs AS vt_s, ve AS vt_e
ORDER BY tt, Person1Id, Person2Id, change, vt_s
