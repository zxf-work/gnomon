/* BI13, window setting (rule W1). Deviation: Zombies is a stage over (country, city, person) bindings with the
   messages as an optional part co-valid with them (W_IN); the like counts are a stage over (liker, like,
   message) bindings (W_OK), joined on the zombie key (rule 5). */
WITH Zombies AS (
    SELECT Person.id AS zombieid
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN Person
        ON Person.LocationCityId = City.id
      LEFT JOIN Message
         ON Person.id = Message.CreatorPersonId
        AND Message.creationDate BETWEEN Person.creationDate AND :endDate
        AND W_IN(W_LO(Country, City, Person), W_HI(Country, City, Person), Message)
     WHERE Country.name = :country
       AND Person.creationDate < :endDate
       AND W_OK(Country, City, Person)
     GROUP BY Person.id, Person.creationDate, Person.vt_s
    HAVING count(Message.MessageId) < 12*extract(YEAR FROM :endDate) + extract(MONTH FROM :endDate)
                            - (12*extract(YEAR FROM Person.creationDate) + extract(MONTH FROM Person.creationDate))
                            + 1
)
SELECT Z.zombieid AS "zombie.id"
     , coalesce(t.zombieLikeCount, 0) AS zombieLikeCount
     , coalesce(t.totalLikeCount, 0) AS totalLikeCount
     , CASE WHEN t.totalLikeCount > 0 THEN t.zombieLikeCount::float/t.totalLikeCount ELSE 0 END AS zombieScore
  FROM Zombies Z LEFT JOIN (
    SELECT Z.zombieid, count(*) as totalLikeCount, sum(case when exists (SELECT 1 FROM Zombies ZL WHERE ZL.zombieid = p.id) then 1 else 0 end) AS zombieLikeCount
    FROM Person p, Person_likes_Message plm, Message m, Zombies Z
    WHERE Z.zombieid = m.CreatorPersonId AND p.creationDate < :endDate
        AND p.id = plm.PersonId AND m.MessageId = plm.MessageId
        AND W_OK(p, plm, m)
    GROUP BY Z.zombieid
  ) t ON (Z.zombieid = t.zombieid)
 ORDER BY zombieScore DESC, Z.zombieid
 LIMIT 100
;
