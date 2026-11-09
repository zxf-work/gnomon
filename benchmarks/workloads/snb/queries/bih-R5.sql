/* BIH-R5. Temporal join (TPC-BiH R5 on SNB): per Person whose version had browserUsed = :browser, the number of
   Messages they created whose versions were valid together with that Person version at some instant of
   [:A, :B), as believed at :T (sequenced on valid time). Top 20. Ours, not LDBC's. */
SELECT p.id, count(*) AS n
FROM Person_h p JOIN Message_h m ON m.CreatorPersonId = p.id
WHERE p.tt_s <= :T AND :T < p.tt_e AND m.tt_s <= :T AND :T < m.tt_e
  AND p.browserUsed = :browser
  AND greatest(p.vt_s, m.vt_s, :A) < least(p.vt_e, m.vt_e, :B)
GROUP BY p.id
ORDER BY n DESC, p.id
LIMIT 20
;
