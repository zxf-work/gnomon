/* IC3, window setting (rule W1). Deviation: the friend set is W_REACH (1..2 hops) against the (person, city,
   country) binding; the two counts are correlated parts of that binding (G11 verdict 3): a message counts when it
   shares an instant with the person, the city, the country and a co-valid walk. */
SELECT
    Person.id AS otherPersonId,
    Person.firstName AS otherPersonFirstName,
    Person.lastName AS otherPersonLastName,
    cpc.ct1 AS xCount,
    cpc.ct2 AS yCount,
    cpc.ct1 + cpc.ct2 AS count
FROM Person, City, Country,
LATERAL (
    SELECT
        (SELECT count(DISTINCT m.id) FROM Message m, Country cx
          WHERE m.CreatorPersonId = Person.id AND m.LocationCountryId = cx.id AND cx.name = :countryXName
            AND m.creationDate >= :startDate AND m.creationDate < (:startDate + INTERVAL '1 days' * :durationDays)
            AND W_REACH(:personId, Person.id, 2, W_LO(Person, City, Country, m, cx), W_HI(Person, City, Country, m, cx))) AS ct1,
        (SELECT count(DISTINCT m.id) FROM Message m, Country cy
          WHERE m.CreatorPersonId = Person.id AND m.LocationCountryId = cy.id AND cy.name = :countryYName
            AND m.creationDate >= :startDate AND m.creationDate < (:startDate + INTERVAL '1 days' * :durationDays)
            AND W_REACH(:personId, Person.id, 2, W_LO(Person, City, Country, m, cy), W_HI(Person, City, Country, m, cy))) AS ct2
) cpc
WHERE Person.LocationCityId = City.id
  AND City.PartOfCountryId = Country.id
  AND Country.name <> :countryXName
  AND Country.name <> :countryYName
  AND Person.id <> :personId
  AND W_OK(Person, City, Country)
  AND W_REACH(:personId, Person.id, 2, W_LO(Person, City, Country), W_HI(Person, City, Country))
  AND cpc.ct1 > 0 AND cpc.ct2 > 0
ORDER BY cpc.ct1 + cpc.ct2 DESC, Person.id ASC
LIMIT 20
;
