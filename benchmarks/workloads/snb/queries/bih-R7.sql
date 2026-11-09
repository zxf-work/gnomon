/* BIH-R7. Changes between two belief versions (TPC-BiH R7 / snapshot diff on SNB): KNOWS versions added
   (tt_s in (:T1, :T]) or retracted (tt_e in (:T1, :T]) whose valid time overlaps [:A, :B). Ours, not LDBC's. */
SELECT Person1Id, Person2Id, change, tt, vt_s, vt_e FROM (
  SELECT Person1Id, Person2Id, 'added' AS change, tt_s AS tt, vt_s, vt_e FROM Person_knows_Person_h
   WHERE tt_s > :T1 AND tt_s <= :T AND vt_s < :B AND :A < vt_e
  UNION ALL
  SELECT Person1Id, Person2Id, 'retracted', tt_e, vt_s, vt_e FROM Person_knows_Person_h
   WHERE tt_e > :T1 AND tt_e <= :T AND vt_s < :B AND :A < vt_e
) x
ORDER BY tt, Person1Id, Person2Id, change, vt_s
;
