// BIH-R7 (Cypher form of queries/bih-R7.sql). KNOWS edge versions added (tt_s in ($T1, $T]) or retracted (tt_e in
// ($T1, $T]) whose valid time overlaps [$A, $B). Edge versions by their own rectangle; DISTINCT folds the copies.
// KNOWS is stored once, source id < target id, as in the SQL table. Ours, not LDBC's.
MATCH (a:Person)-[k:KNOWS]->(b:Person)
WHERE k.evs < $B AND $A < k.eve AND ((k.ets > $T1 AND k.ets <= $T) OR (k.ete > $T1 AND k.ete <= $T))
WITH DISTINCT a.id AS p1, b.id AS p2, k.evs AS vs, k.eve AS ve, k.ets AS ts, k.ete AS te
UNWIND [x IN [['added', ts], ['retracted', te]] WHERE x[1] > $T1 AND x[1] <= $T] AS ch
RETURN p1 AS Person1Id, p2 AS Person2Id, ch[0] AS change, ch[1] AS tt, vs AS vt_s, ve AS vt_e
ORDER BY tt, Person1Id, Person2Id, change, vt_s
