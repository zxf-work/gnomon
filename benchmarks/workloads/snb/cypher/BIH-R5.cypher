// BIH-R5 (Cypher form of queries/bih-R5.sql). Per Person whose version had browserUsed = $browser, the number of
// (Person version, Message version) pairs of Messages they created, both believed at $T, whose valid times meet
// inside [$A, $B) (sequenced on valid time). A HAS_CREATOR copy exists for exactly each pair of versions with
// intersecting rectangles, so count(*) over the copies is the SQL join's count. Top 20. Ours, not LDBC's.
MATCH (m:Message)-[c:HAS_CREATOR]->(p:Person)
WHERE p.browserUsed = $browser AND p.tt_s <= $T AND $T < p.tt_e AND m.tt_s <= $T AND $T < m.tt_e
  AND p.vt_s < $B AND m.vt_s < $B AND $A < p.vt_e AND $A < m.vt_e AND p.vt_s < m.vt_e AND m.vt_s < p.vt_e
RETURN p.id AS id, count(*) AS n
ORDER BY n DESC, id
LIMIT 20
