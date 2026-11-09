// BIH-K for LadybugDB (instant settings): written by scripts/w3_ladybug_texts.py from BIH-K.cypher.
// Mechanical rules (DEVIATIONS D-W3-13): none.
MATCH (p:Person {id: $personId})
WHERE p.pts <= $T AND p.pvs < $B AND $A < p.pve
RETURN DISTINCT p.id AS id, p.firstName AS firstName, p.lastName AS lastName, p.browserUsed AS browserUsed,
       p.locationIP AS locationIP, p.pvs AS vt_s, p.pve AS vt_e, p.pts AS tt_s,
       CASE WHEN p.pte > $T THEN 4611686018427387904 ELSE p.pte END AS tt_e
ORDER BY tt_s, vt_s
