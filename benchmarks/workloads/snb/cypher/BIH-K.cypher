// BIH-K (Cypher form of queries/bih-K.sql). Every version of one Person written by $T whose valid time overlaps
// [$A, $B), on both clocks; tt_e censored to OPEN_END above $T. A Person node is a piece of a version (split by
// the merged location FK); each piece carries its whole version's rectangle (pvs, pve, pts, pte), and DISTINCT
// folds the pieces into the versions. Ours, not LDBC's.
MATCH (p:Person {id: $personId})
WHERE p.pts <= $T AND p.pvs < $B AND $A < p.pve
RETURN DISTINCT p.id AS id, p.firstName AS firstName, p.lastName AS lastName, p.browserUsed AS browserUsed,
       p.locationIP AS locationIP, p.pvs AS vt_s, p.pve AS vt_e, p.pts AS tt_s,
       CASE WHEN p.pte > $T THEN 4611686018427387904 ELSE p.pte END AS tt_e
ORDER BY tt_s, vt_s
