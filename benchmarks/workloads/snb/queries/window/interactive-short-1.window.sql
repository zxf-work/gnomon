/* IS1, sequenced over the valid window [:VA, :VB) as believed at :T (views bi/iv2_window: overlapping versions).
   Deviation from LDBC's text: none needed (one table; LocationCityId is the merged FK of the same version). */
SELECT firstName, lastName, birthday, locationIP, browserUsed, LocationCityId, gender, creationDate
FROM Person
WHERE id = :personId
;
