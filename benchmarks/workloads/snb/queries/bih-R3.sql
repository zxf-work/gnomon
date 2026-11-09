/* BIH-R3. Temporal aggregation (TPC-BiH R3 on SNB): the number of KNOWS versions valid at the start of each
   day of [:A, :B), as believed at :T. Ours, not LDBC's. */
SELECT d AS bucket,
       (SELECT count(*) FROM Person_knows_Person_h k
         WHERE k.tt_s <= :T AND :T < k.tt_e AND k.vt_s <= d AND d < k.vt_e) AS n
FROM (SELECT :A + i * 86400000000 AS d FROM generate_series(0, CAST(floor((:B - :A - 1) / 86400000000) AS BIGINT)) AS g(i)) days
ORDER BY bucket
;
