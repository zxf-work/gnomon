/* BI19, window setting (rule W1, W1-paths). Deviation: Umbra's precomputed PathQ19 weights are a stage over valid
   (friendship, message, reply) bindings (rule 5) joined to each friendship on its key; the cheapest paths
   from every person of city 1 to every person of city 2 are searched over co-valid walks carrying (lo, hi),
   by W_SEARCH (8 rounds); all pairs at the minimum are returned. */
WITH
weights(src, dst, w) AS (
    SELECT pp.person1id, pp.person2id, greatest(round(40 - sqrt(count(*)))::bigint, 1)
    FROM (SELECT * FROM Person_knows_person WHERE person1id < person2id) pp, Message m1, Message m2
    WHERE pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid) AND pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid)
      AND m1.parentmessageid = m2.messageid AND m1.creatorpersonid <> m2.creatorpersonid
      AND W_OK(pp, m1, m2)
    GROUP BY pp.person1id, pp.person2id
),
edges(src, dst, w, lo, hi) AS (
    SELECT k.Person1Id, k.Person2Id, wt.w::double precision, W_LO(k, q), W_HI(k, q)
    FROM Person_knows_Person k JOIN Person q ON q.id = k.Person2Id
    JOIN weights wt ON wt.src = least(k.Person1Id, k.Person2Id) AND wt.dst = greatest(k.Person1Id, k.Person2Id)
    WHERE W_OK(k, q)
),
W_SEARCH(search, (SELECT p.id, p.id, 0::double precision, W_LO(p), W_HI(p) FROM Person p WHERE p.LocationCityId = :city1Id AND W_OK(p)), edges, 8),
results(f, t, w) AS (
    SELECT s.f, s.node, min(s.cost) FROM search s JOIN Person t ON t.id = s.node
    WHERE t.LocationCityId = :city2Id AND greatest(t.vt_s, s.lo) < least(t.vt_e, s.hi)
    GROUP BY s.f, s.node
)
SELECT * FROM results WHERE w = (SELECT min(w) FROM results) ORDER BY f, t
;
