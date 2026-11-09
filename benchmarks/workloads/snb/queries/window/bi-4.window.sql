/* BI4, window setting (rule W1). Deviation: Umbra's precomputed Top100PopularForumsQ04 is computed here as a stage
   over its own valid bindings (rule 5); the binding is (person, top-forum membership), and a left-joined message
   counts when it shares an instant with the person and such a membership: the EXISTS's common interval is carried
   into the message test (G12 verdict 2). */
WITH ForumMembershipPerCountry AS (
    SELECT fm.ForumId AS ForumId, count(Person.id) AS numberOfMembers, City.PartOfCountryId AS CountryId
      FROM Forum_hasMember_Person fm JOIN Person ON Person.id = fm.PersonId JOIN City ON City.id = Person.LocationCityId
     WHERE W_OK(fm, Person, City)
     GROUP BY City.PartOfCountryId, fm.ForumId
), Top100_Popular_Forums AS (
  SELECT Forum.id, Forum.creationDate, max(numberOfMembers) AS maxNumberOfMembers
  FROM ForumMembershipPerCountry fc JOIN Forum ON Forum.id = fc.ForumId
  WHERE Forum.creationDate > :date
  GROUP BY Forum.id, Forum.creationDate
  ORDER BY maxNumberOfMembers DESC, id
  LIMIT 100
)
SELECT au.id AS "person.id"
     , au.firstName AS "person.firstName"
     , au.lastName AS "person.lastName"
     , au.creationDate
     , count(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM Top100_Popular_Forums tf2
                                        JOIN Forum_hasMember_Person fm2 ON fm2.ForumId = tf2.id
                                        WHERE fm2.PersonId = au.id AND W_IN(W_LO(au, Message), W_HI(au, Message), fm2))
                     THEN Message.MessageId END) AS messageCount
  FROM
       Person au
       LEFT JOIN Message
              ON au.id = Message.CreatorPersonId
             AND Message.ContainerForumId IN (SELECT id FROM Top100_Popular_Forums)
             AND W_IN(W_LO(au), W_HI(au), Message)
  WHERE EXISTS (SELECT 1
                FROM Top100_Popular_Forums
                INNER JOIN Forum_hasMember_Person
                        ON Forum_hasMember_Person.ForumId = Top100_Popular_Forums.id
                WHERE Forum_hasMember_Person.PersonId = au.id
                  AND W_IN(W_LO(au), W_HI(au), Forum_hasMember_Person)
               )
GROUP BY au.id, au.firstName, au.lastName, au.creationDate
ORDER BY messageCount DESC, au.id
LIMIT 100
;
