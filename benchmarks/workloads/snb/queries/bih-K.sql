/* BIH-K. Entity history (TPC-BiH K1 on SNB): every version of one Person written by :T, on both clocks,
   restricted to versions whose valid time overlaps [:A, :B). Ours, not LDBC's. tt_e is censored to OPEN_END above :T. */
SELECT id, firstName, lastName, browserUsed, locationIP, vt_s, vt_e, tt_s,
       CASE WHEN tt_e > :T THEN 4611686018427387904 ELSE tt_e END AS tt_e
FROM Person_v
WHERE id = :personId AND tt_s <= :T AND vt_s < :B AND :A < vt_e
ORDER BY tt_s, vt_s
;
