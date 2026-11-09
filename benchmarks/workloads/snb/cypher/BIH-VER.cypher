// BIH-VER (Cypher form of queries/bih-VER.sql). For one Person, each friendship version valid at $V and written by
// $T, with its belief interval (tt_e censored above $T). A KNOWS edge version is read from its own rectangle
// (evs/eve/ets/ete, kept on every relationship copy); DISTINCT folds the copies. Ours, not LDBC's.
MATCH (p:Person {id: $personId})-[k:KNOWS]-(f:Person)
WHERE k.ets <= $T AND k.evs <= $V AND $V < k.eve
RETURN DISTINCT f.id AS friendId, k.ets AS tt_s,
       CASE WHEN k.ete > $T THEN 4611686018427387904 ELSE k.ete END AS tt_e
ORDER BY friendId, tt_s
