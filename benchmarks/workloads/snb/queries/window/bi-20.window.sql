/* BI20, window setting (rule W1, W1-paths). Deviation: Umbra's precomputed PathQ20 weights are a stage over valid
   (friendship, two studyAt edges) bindings (rule 5), joined to each friendship on its key; the targets are a
   stage over valid (workAt, company) bindings; the cheapest paths from person2 are searched over co-valid
   walks carrying (lo, hi), by W_SEARCH (8 rounds); the targets at the minimum are returned. */
WITH
weights(src, dst, w) AS (
    SELECT p1.personid, p2.personid, min(abs(p1.classYear - p2.classYear)) + 1
    FROM Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
    WHERE pp.person1id = p1.personid AND pp.person2id = p2.personid AND p1.universityid = p2.universityid
      AND W_OK(pp, p1, p2)
    GROUP BY p1.personid, p2.personid
),
dsts(t) AS (
    SELECT pwc.personid FROM Person_workat_company pwc, Company c
    WHERE pwc.companyid = c.id AND c.name = :company AND W_OK(pwc, c)
),
edges(src, dst, w, lo, hi) AS (
    SELECT k.Person1Id, k.Person2Id, wt.w, W_LO(k, q), W_HI(k, q)
    FROM Person_knows_Person k JOIN Person q ON q.id = k.Person2Id
    JOIN weights wt ON wt.src = k.Person1Id AND wt.dst = k.Person2Id
    WHERE W_OK(k, q)
),
W_SEARCH(search, (SELECT p.id, p.id, 0, W_LO(p), W_HI(p) FROM Person p WHERE p.id = :person2Id AND W_OK(p)), edges, 8),
results(t, w) AS (
    SELECT s.node, min(s.cost) FROM search s WHERE s.node IN (SELECT t FROM dsts) GROUP BY s.node
)
SELECT t, w FROM results WHERE w = (SELECT min(w) FROM results) ORDER BY t LIMIT 20
;
