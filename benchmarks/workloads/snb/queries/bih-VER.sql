/* BIH-VER. "Which versions" (BEAR Ver on SNB): for one Person, each friendship version valid at the instant :V
   and written by :T, with its belief interval (tt_e censored to OPEN_END above :T). Ours, not LDBC's. */
SELECT DISTINCT friendId, tt_s, tt_e FROM (
  SELECT Person2Id AS friendId, tt_s, CASE WHEN tt_e > :T THEN 4611686018427387904 ELSE tt_e END AS tt_e
    FROM Person_knows_Person_h WHERE Person1Id = :personId AND tt_s <= :T AND vt_s <= :V AND :V < vt_e
  UNION ALL
  SELECT Person1Id, tt_s, CASE WHEN tt_e > :T THEN 4611686018427387904 ELSE tt_e END
    FROM Person_knows_Person_h WHERE Person2Id = :personId AND tt_s <= :T AND vt_s <= :V AND :V < vt_e
) x
ORDER BY friendId, tt_s
;
