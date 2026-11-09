/* BI18, window setting (rule W1). Deviation: one binding of both interests (with their tag) and both friendships
   to the mutual friend, co-valid (W_OK); "the two are not friends" is W_NOT. */
SELECT k1.Person1Id AS "person1.id", k2.Person1Id AS "person2.id", count(k1.Person2Id) AS mutualFriendCount
FROM Person_hasInterest_Tag pt1
JOIN Tag t1 ON t1.id = pt1.TagId AND t1.name = :tag
JOIN Person_knows_Person k1 ON k1.Person1Id = pt1.PersonId
JOIN Person_knows_Person k2 ON k2.Person2Id = k1.Person2Id
JOIN Person_hasInterest_Tag pt2 ON pt2.PersonId = k2.Person1Id
JOIN Tag t2 ON t2.id = pt2.TagId AND t2.name = :tag
WHERE k1.Person1Id != k2.Person1Id
  AND W_OK(pt1, t1, k1, k2, pt2, t2)
  AND W_NOT(W_LO(pt1, t1, k1, k2, pt2, t2), W_HI(pt1, t1, k1, k2, pt2, t2),
            (FROM Person_knows_Person k3 WHERE k3.Person1Id = k2.Person1Id AND k3.Person2Id = k1.Person1Id), (k3))
GROUP BY k1.Person1Id, k2.Person1Id
ORDER BY mutualFriendCount DESC, k1.Person1Id ASC, k2.Person1Id ASC
LIMIT 20
;
