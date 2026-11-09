/* IC1, window setting (rule W1). Deviation: the distance is W_DIST (the smallest depth of a co-valid KNOWS
   walk, up to 3, from the start person), tested against the (person, city) binding; the university and
   company lists are correlated parts of that binding (G11 verdict 3): an entry is listed when it shares an instant
   with the person, the city and a co-valid walk of at most 3 hops. Column lists as the reference. */
SELECT
    Person.id,
    Person.lastName,
    min(d.distance) AS distance,
    Person.birthday,
    Person.creationDate,
    Person.gender,
    Person.browserUsed,
    Person.locationIP,
    string_to_array(Person.email, ';') AS emails,
    string_to_array(Person.speaks, ';') AS languages,
    City.name,
    (
        SELECT array_agg(ARRAY[University.name, Person_studyAt_University.classYear::text, uc.name])
        FROM Person_studyAt_University, University, City uc
        WHERE Person_studyAt_University.PersonId = Person.id
          AND Person_studyAt_University.UniversityId = University.id
          AND University.LocationPlaceId = uc.id
          AND W_REACH(:personId, Person.id, 3, W_LO(Person, City, Person_studyAt_University, University, uc),
                      W_HI(Person, City, Person_studyAt_University, University, uc))
    ) AS university,
    (
        SELECT array_agg(ARRAY[Company.name, Person_workAt_Company.workFrom::text, Country.name])
        FROM Person_workAt_Company, Company, Country
        WHERE Person_workAt_Company.PersonId = Person.id
          AND Person_workAt_Company.CompanyId = Company.id
          AND Company.LocationPlaceId = Country.id
          AND W_REACH(:personId, Person.id, 3, W_LO(Person, City, Person_workAt_Company, Company, Country),
                      W_HI(Person, City, Person_workAt_Company, Company, Country))
    ) AS company
FROM Person, City,
     LATERAL (SELECT W_DIST(:personId, Person.id, 3, W_LO(Person, City), W_HI(Person, City)) AS distance) d
WHERE Person.LocationCityId = City.id
  AND Person.firstName = :firstName
  AND Person.id <> :personId
  AND W_OK(Person, City)
  AND d.distance IS NOT NULL
GROUP BY Person.id, Person.lastName, Person.birthday, Person.creationDate, Person.gender, Person.browserUsed,
         Person.locationIP, Person.email, Person.speaks, City.name, Person.vt_s, Person.vt_e, City.vt_s, City.vt_e
ORDER BY distance, Person.lastName, Person.id
LIMIT 20
;
