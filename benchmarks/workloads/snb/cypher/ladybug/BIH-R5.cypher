// BIH-R5 for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BIH-R5.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): R1.
MATCH (m:Post:Comment)-[c:HAS_CREATOR]->(p:Person)
WHERE p.browserUsed = $browser AND p.tt_s <= $T AND $T < p.tt_e AND m.tt_s <= $T AND $T < m.tt_e
  AND p.vt_s < $B AND m.vt_s < $B AND $A < p.vt_e AND $A < m.vt_e AND p.vt_s < m.vt_e AND m.vt_s < p.vt_e
RETURN p.id AS id, count(*) AS n
ORDER BY n DESC, id
LIMIT 20
